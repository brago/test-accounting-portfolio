/*
 * Copyright 2017 The Mifos Initiative.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mifos;

import com.google.gson.Gson;
import io.mifos.accounting.api.v1.client.LedgerManager;
import io.mifos.accounting.api.v1.domain.*;
import io.mifos.accounting.importer.AccountImporter;
import io.mifos.accounting.importer.LedgerImporter;
import io.mifos.core.api.config.EnableApiFactory;
import io.mifos.core.api.context.AutoUserContext;
import io.mifos.core.api.util.ApiFactory;
import io.mifos.core.lang.DateConverter;
import io.mifos.core.lang.DateRange;
import io.mifos.core.test.env.ExtraProperties;
import io.mifos.core.test.fixture.cassandra.CassandraInitializer;
import io.mifos.core.test.fixture.mariadb.MariaDBInitializer;
import io.mifos.core.test.listener.EnableEventRecording;
import io.mifos.core.test.listener.EventRecorder;
import io.mifos.core.test.servicestarter.ActiveMQForTest;
import io.mifos.core.test.servicestarter.EurekaForTest;
import io.mifos.core.test.servicestarter.InitializedMicroservice;
import io.mifos.core.test.servicestarter.IntegrationTestEnvironment;
import io.mifos.individuallending.api.v1.domain.caseinstance.CaseParameters;
import io.mifos.individuallending.api.v1.domain.caseinstance.CreditWorthinessFactor;
import io.mifos.individuallending.api.v1.domain.caseinstance.CreditWorthinessSnapshot;
import io.mifos.individuallending.api.v1.domain.product.AccountDesignators;
import io.mifos.individuallending.api.v1.domain.product.ChargeIdentifiers;
import io.mifos.individuallending.api.v1.domain.product.ProductParameters;
import io.mifos.individuallending.api.v1.domain.workflow.Action;
import io.mifos.individuallending.api.v1.events.IndividualLoanCommandEvent;
import io.mifos.individuallending.api.v1.events.IndividualLoanEventConstants;
import io.mifos.portfolio.api.v1.client.PortfolioManager;
import io.mifos.portfolio.api.v1.client.ProductDefinitionIncomplete;
import io.mifos.portfolio.api.v1.domain.*;
import io.mifos.portfolio.api.v1.events.*;
import io.mifos.rhythm.spi.v1.client.BeatListener;
import io.mifos.rhythm.spi.v1.domain.BeatPublish;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.test.context.junit4.SpringRunner;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.mifos.individuallending.api.v1.domain.product.AccountDesignators.*;
import static io.mifos.portfolio.api.v1.events.EventConstants.PUT_PRODUCT;


@SuppressWarnings("SpringAutowiredFieldsWarningInspection")
@RunWith(SpringRunner.class)
@SpringBootTest()
public class IndividualLoanTransactionProcessing {
  private static final String SCHEDULER_USER_NAME = "imhotep";
  private static final String TEST_LOGGER = "test-logger";

  private static final int MINOR_CURRENCY_UNIT_DIGITS = 2;
  private static final BigDecimal PROCESSING_FEE_AMOUNT = BigDecimal.valueOf(10_0000, MINOR_CURRENCY_UNIT_DIGITS);
  private static final BigDecimal LOAN_ORIGINATION_FEE_AMOUNT = BigDecimal.valueOf(100_0000, MINOR_CURRENCY_UNIT_DIGITS);
  private static final BigDecimal DISBURSEMENT_FEE_AMOUNT = BigDecimal.valueOf(1_0000, MINOR_CURRENCY_UNIT_DIGITS);

  private static final BigDecimal INTEREST_RATE = BigDecimal.valueOf(0.10).setScale(4, RoundingMode.HALF_EVEN);
  private static final BigDecimal ACCRUAL_PERIODS = BigDecimal.valueOf(365.2425);

  private static final String CUSTOMER_LOAN_LEDGER_IDENTIFIER = "7010";

  private static final String LOAN_FUNDS_SOURCE_ACCOUNT_IDENTIFIER = "7310.1";
  private static final String LOAN_ORIGINATION_FEES_ACCOUNT_IDENTIFIER = "1310";
  private static final String PROCESSING_FEE_INCOME_ACCOUNT_IDENTIFIER = "1312";
  private static final String DISBURSEMENT_FEE_INCOME_ACCOUNT_IDENTIFIER = "1313";
  private static final String TELLER_ONE_ACCOUNT_IDENTIFIER = "7352";
  private static final String LOAN_INTEREST_ACCRUAL_ACCOUNT_IDENTIFIER = "7810";
  private static final String CONSUMER_LOAN_INTEREST_ACCOUNT_IDENTIFIER = "1103";
  private static final String LOANS_PAYABLE_ACCOUNT_IDENTIFIER ="8530";
  private static final String LATE_FEE_INCOME_ACCOUNT_IDENTIFIER = "1311";
  private static final String LATE_FEE_ACCRUAL_ACCOUNT_IDENTIFIER = "7840";
  private static final String ARREARS_ALLOWANCE_ACCOUNT_IDENTIFIER = "3010";
  private static final String PENDING_DISBURSAL_ACCOUNT_IDENTIFIER = "7310.2";

  @Configuration
  @ActiveMQForTest.EnableActiveMQListen
  @EnableApiFactory
  @ComponentScan("io.mifos.listener")
  public static class TestConfiguration {
    public TestConfiguration() {
      super();
    }

    @Bean(name= TEST_LOGGER)
    public Logger logger() {
      return LoggerFactory.getLogger(TEST_LOGGER);
    }
  }


  private final static EurekaForTest eurekaForTest = new EurekaForTest();
  private final static ActiveMQForTest activeMQForTest = new ActiveMQForTest();
  private final static CassandraInitializer cassandraInitializer = new CassandraInitializer();
  private final static MariaDBInitializer mariaDBInitializer = new MariaDBInitializer();
  private final static IntegrationTestEnvironment integrationTestEnvironment = new IntegrationTestEnvironment(cassandraInitializer, mariaDBInitializer);

  private final static InitializedMicroservice<LedgerManager> thoth = new InitializedMicroservice<>(LedgerManager.class, "accounting", "0.1.0-BUILD-SNAPSHOT", integrationTestEnvironment);
  private final static InitializedMicroservice<PortfolioManager> bastet= new InitializedMicroservice<>(PortfolioManager.class, "portfolio", "0.1.0-BUILD-SNAPSHOT", integrationTestEnvironment)
      .addProperties(new ExtraProperties() {{
        setProperty("portfolio.bookInterestAsUser", SCHEDULER_USER_NAME);
      }});

  @ClassRule
  public static TestRule orderedRules = RuleChain
          .outerRule(eurekaForTest)
          .around(activeMQForTest)
          .around(cassandraInitializer)
          .around(mariaDBInitializer)
          .around(integrationTestEnvironment)
          .around(thoth)
          .around(bastet);

  @Autowired
  private ApiFactory apiFactory;

  @Autowired
  protected EventRecorder eventRecorder;

  @Autowired
  @Qualifier(TEST_LOGGER)
  protected Logger logger;

  private AutoUserContext userContext;

  private BeatListener portfolioBeatListener;

  private BigDecimal expectedCurrentBalance = null;
  private BigDecimal interestAccrued = BigDecimal.ZERO;

  private Product product = null;
  private TaskDefinition taskDefinition = null;
  private CaseParameters caseParameters = null;
  private Case customerCase = null;
  private String customerLoanAccountIdentifier = null;

  public IndividualLoanTransactionProcessing() {
    super();
  }

  @Before
  public void before() throws InterruptedException {
    bastet.setApiFactory(apiFactory);
    thoth.setApiFactory(apiFactory);
    portfolioBeatListener = new ApiFactory(logger).create(BeatListener.class, bastet.getProcessEnvironment().serverURI());

    userContext = integrationTestEnvironment.createAutoUserContext("blah");

    try {
      final LedgerImporter ledgerImporter = new LedgerImporter(thoth.api(), logger);
      final URL ledgersUrl = ClassLoader.getSystemResource("standardChartOfAccounts/ledgers.csv");
      Assert.assertNotNull(ledgersUrl);
      ledgerImporter.importCSV(ledgersUrl);

      Assert.assertTrue(eventRecorder.wait(io.mifos.accounting.api.v1.EventConstants.POST_LEDGER, CUSTOMER_LOAN_LEDGER_IDENTIFIER));

      final AccountImporter accountImporter = new AccountImporter(thoth.api(), logger);
      final URL accountsUrl = ClassLoader.getSystemResource("standardChartOfAccounts/accounts.csv");
      Assert.assertNotNull(accountsUrl);
      accountImporter.importCSV(accountsUrl);

      Assert.assertTrue(eventRecorder.wait(io.mifos.accounting.api.v1.EventConstants.POST_ACCOUNT, DISBURSEMENT_FEE_INCOME_ACCOUNT_IDENTIFIER));
      Assert.assertTrue(eventRecorder.wait(io.mifos.accounting.api.v1.EventConstants.POST_ACCOUNT, PROCESSING_FEE_INCOME_ACCOUNT_IDENTIFIER));
      Assert.assertTrue(eventRecorder.wait(io.mifos.accounting.api.v1.EventConstants.POST_ACCOUNT, PENDING_DISBURSAL_ACCOUNT_IDENTIFIER));
    } catch (IOException e) {
      Assert.fail("Failed to import chart of accounts.");
    }
  }

  @After
  public void after()
  {
    userContext.close();
  }

  @Test
  public void testAccountAssignments() throws InterruptedException {
    final List<Pattern> patterns = bastet.api().getAllPatterns();
    Assert.assertTrue(patterns != null);
    Assert.assertTrue(patterns.size() >= 1);
    Assert.assertTrue(patterns.get(0) != null);
    Assert.assertEquals(patterns.get(0).getParameterPackage(), "io.mifos.individuallending.api.v1");

    final Product product = defineProductWithoutAccountAssignments();

    bastet.api().createProduct(product);
    Assert.assertTrue(this.eventRecorder.wait(EventConstants.POST_PRODUCT, product.getIdentifier()));
    final Set<AccountAssignment> incompleteAccountAssignments = bastet.api().getIncompleteAccountAssignments(product.getIdentifier());
    Assert.assertTrue(!incompleteAccountAssignments.isEmpty());
    try {
      bastet.api().enableProduct(product.getIdentifier(), true);
      Assert.fail("Enable shouldn't work without the account assignments.");
    }
    catch (final ProductDefinitionIncomplete ignored2) { }

    final Product changedProduct = bastet.api().getProduct(product.getIdentifier());

    changedProduct.setAccountAssignments(defineAccountAssignments());

    bastet.api().changeProduct(changedProduct.getIdentifier(), changedProduct);
    Assert.assertTrue(this.eventRecorder.wait(PUT_PRODUCT, changedProduct.getIdentifier()));
  }

  @Test
  public void workflowTerminatingInApplicationDenial() throws InterruptedException {
    step1CreateProduct();
    step2CreateCase();
    step3OpenCase();
    step4DenyCase();
  }

  @Test
  public void workflowTerminatingInEarlyLoanPayoff() throws InterruptedException {
    step1CreateProduct();
    step2CreateCase();
    step3OpenCase();
    step4ApproveCase();
    step5Disburse(BigDecimal.valueOf(2000L));
    //TODO:step6CalculateInterestAccrual();
    step7PaybackPartialAmount(expectedCurrentBalance);
    step8Close();
  }


  @Test
  public void workflowWithTwoNearlyEqualRepayments() throws InterruptedException {
    step1CreateProduct();
    step2CreateCase();
    step3OpenCase();
    step4ApproveCase();
    step5Disburse(BigDecimal.valueOf(2000L));
    //TODO:step6CalculateInterestAccrual();
    final BigDecimal repayment1 = expectedCurrentBalance.divide(BigDecimal.valueOf(2), BigDecimal.ROUND_HALF_EVEN);
    step7PaybackPartialAmount(repayment1);
    step7PaybackPartialAmount(expectedCurrentBalance);
    step8Close();
  }

  //Create product and set charges to fixed fees.
  private void step1CreateProduct() throws InterruptedException {
    logger.info("step1CreateProduct");
    product = createProduct();

    setFeeToFixedValue(product.getIdentifier(), ChargeIdentifiers.PROCESSING_FEE_ID, PROCESSING_FEE_AMOUNT);
    setFeeToFixedValue(product.getIdentifier(), ChargeIdentifiers.LOAN_ORIGINATION_FEE_ID, LOAN_ORIGINATION_FEE_AMOUNT);
    setFeeToFixedValue(product.getIdentifier(), ChargeIdentifiers.DISBURSEMENT_FEE_ID, DISBURSEMENT_FEE_AMOUNT);

    final ChargeDefinition interestChargeDefinition = bastet.api().getChargeDefinition(product.getIdentifier(), ChargeIdentifiers.INTEREST_ID);
    interestChargeDefinition.setAmount(INTEREST_RATE);

    bastet.api().changeChargeDefinition(product.getIdentifier(), interestChargeDefinition.getIdentifier(), interestChargeDefinition);
    Assert.assertTrue(this.eventRecorder.wait(EventConstants.PUT_CHARGE_DEFINITION,
        new ChargeDefinitionEvent(product.getIdentifier(), interestChargeDefinition.getIdentifier())));

    taskDefinition = createTaskDefinition(product);

    bastet.api().enableProduct(product.getIdentifier(), true);
    Assert.assertTrue(this.eventRecorder.wait(EventConstants.PUT_PRODUCT_ENABLE, product.getIdentifier()));
  }

  private void step2CreateCase() throws InterruptedException {
    logger.info("step2CreateCase");
    caseParameters = getCaseParameters();
    final String caseParametersAsString = new Gson().toJson(caseParameters);
    customerCase = createAdjustedCase(product.getIdentifier(), x -> x.setParameters(caseParametersAsString));
  }

  //Open the case and accept a processing fee.
  private void step3OpenCase() throws InterruptedException {
    logger.info("step3OpenCase");
    checkStateTransfer(
        product.getIdentifier(),
        customerCase.getIdentifier(),
        Action.OPEN,
        Collections.singletonList(assignEntryToTeller()),
        IndividualLoanEventConstants.OPEN_INDIVIDUALLOAN_CASE,
        Case.State.PENDING);

    verifyTransfer(TELLER_ONE_ACCOUNT_IDENTIFIER, PROCESSING_FEE_INCOME_ACCOUNT_IDENTIFIER,
        PROCESSING_FEE_AMOUNT, Action.OPEN
    );
  }


  //Deny the case. Once this is done, no more actions are possible for the case.
  private void step4DenyCase() throws InterruptedException {
    logger.info("step4DenyCase");
    checkStateTransfer(
        product.getIdentifier(),
        customerCase.getIdentifier(),
        Action.DENY,
        Collections.singletonList(assignEntryToTeller()),
        IndividualLoanEventConstants.DENY_INDIVIDUALLOAN_CASE,
        Case.State.CLOSED);
  }


  //Approve the case, accept a loan origination fee, and prepare to disburse the loan by earmarking the funds.
  private void step4ApproveCase() throws InterruptedException {
    logger.info("step4ApproveCase");

    markTaskExecuted(product, customerCase, taskDefinition);

    checkStateTransfer(
        product.getIdentifier(),
        customerCase.getIdentifier(),
        Action.APPROVE,
        Collections.singletonList(assignEntryToTeller()),
        IndividualLoanEventConstants.APPROVE_INDIVIDUALLOAN_CASE,
        Case.State.APPROVED);

    customerLoanAccountIdentifier = verifyAccountCreation(CUSTOMER_LOAN_LEDGER_IDENTIFIER, AccountType.ASSET);

    final Set<Debtor> debtors = new HashSet<>();
    debtors.add(new Debtor(LOAN_FUNDS_SOURCE_ACCOUNT_IDENTIFIER, caseParameters.getMaximumBalance().toPlainString()));
    debtors.add(new Debtor(TELLER_ONE_ACCOUNT_IDENTIFIER, LOAN_ORIGINATION_FEE_AMOUNT.toPlainString()));

    final Set<Creditor> creditors = new HashSet<>();
    creditors.add(new Creditor(PENDING_DISBURSAL_ACCOUNT_IDENTIFIER, caseParameters.getMaximumBalance().toPlainString()));
    creditors.add(new Creditor(LOAN_ORIGINATION_FEES_ACCOUNT_IDENTIFIER, LOAN_ORIGINATION_FEE_AMOUNT.toPlainString()));
    verifyTransfer(debtors, creditors, Action.APPROVE);

    expectedCurrentBalance = BigDecimal.ZERO;
  }

  //Approve the case, accept a loan origination fee, and prepare to disburse the loan by earmarking the funds.
  private void step5Disburse(final BigDecimal amount) throws InterruptedException {
    logger.info("step5Disburse");
    checkStateTransfer(
        product.getIdentifier(),
        customerCase.getIdentifier(),
        Action.DISBURSE,
        Collections.singletonList(assignEntryToTeller()),
        amount,
        IndividualLoanEventConstants.DISBURSE_INDIVIDUALLOAN_CASE,
        Case.State.ACTIVE);

    final Set<Debtor> debtors = new HashSet<>();
    debtors.add(new Debtor(PENDING_DISBURSAL_ACCOUNT_IDENTIFIER, amount.toPlainString()));
    debtors.add(new Debtor(LOANS_PAYABLE_ACCOUNT_IDENTIFIER, amount.toPlainString()));
    debtors.add(new Debtor(TELLER_ONE_ACCOUNT_IDENTIFIER, DISBURSEMENT_FEE_AMOUNT.toPlainString()));

    final Set<Creditor> creditors = new HashSet<>();
    creditors.add(new Creditor(customerLoanAccountIdentifier, amount.toPlainString()));
    creditors.add(new Creditor(TELLER_ONE_ACCOUNT_IDENTIFIER, amount.toPlainString()));
    creditors.add(new Creditor(DISBURSEMENT_FEE_INCOME_ACCOUNT_IDENTIFIER, DISBURSEMENT_FEE_AMOUNT.toPlainString()));
    verifyTransfer(debtors, creditors, Action.DISBURSE);

    expectedCurrentBalance = expectedCurrentBalance.add(amount);
  }

  //Perform daily interest calculation.
  private void step6CalculateInterestAccrual() throws InterruptedException {
    logger.info("step6CalculateInterestAccrual");
    final String beatIdentifier = "alignment0";
    final String midnightTimeStamp = DateConverter.toIsoString(LocalDateTime.now().truncatedTo(ChronoUnit.DAYS));


    final BeatPublish interestBeat = new BeatPublish(beatIdentifier, midnightTimeStamp);
    portfolioBeatListener.publishBeat(interestBeat);

    Assert.assertTrue(eventRecorder.wait(IndividualLoanEventConstants.APPLY_INTEREST_INDIVIDUALLOAN_CASE,
        new IndividualLoanCommandEvent(product.getIdentifier(), customerCase.getIdentifier())));

    final Case customerCaseAfterStateChange = bastet.api().getCase(product.getIdentifier(), customerCase.getIdentifier());
    Assert.assertEquals(customerCaseAfterStateChange.getCurrentState(), Case.State.ACTIVE.name());

    final BigDecimal calculatedInterest = caseParameters.getMaximumBalance().multiply(INTEREST_RATE.divide(ACCRUAL_PERIODS, 8, BigDecimal.ROUND_HALF_EVEN))
        .setScale(MINOR_CURRENCY_UNIT_DIGITS, BigDecimal.ROUND_HALF_EVEN);

    interestAccrued = interestAccrued.add(calculatedInterest);

    final Set<Debtor> debtors = new HashSet<>();
    debtors.add(new Debtor(
        customerLoanAccountIdentifier,
        calculatedInterest.toPlainString()));

    final Set<Creditor> creditors = new HashSet<>();
    creditors.add(new Creditor(
        LOAN_INTEREST_ACCRUAL_ACCOUNT_IDENTIFIER,
        calculatedInterest.toPlainString()));
    verifyTransfer(debtors, creditors, Action.APPLY_INTEREST);

    expectedCurrentBalance = expectedCurrentBalance.add(calculatedInterest);
  }

  private void step7PaybackPartialAmount(final BigDecimal amount) throws InterruptedException {
    logger.info("step7PaybackPartialAmount");

    checkStateTransfer(
        product.getIdentifier(),
        customerCase.getIdentifier(),
        Action.ACCEPT_PAYMENT,
        Collections.singletonList(assignEntryToTeller()),
        amount,
        IndividualLoanEventConstants.ACCEPT_PAYMENT_INDIVIDUALLOAN_CASE,
        Case.State.ACTIVE); //Close has to be done explicitly.

    final BigDecimal principal = amount.subtract(interestAccrued);

    final Set<Debtor> debtors = new HashSet<>();
    debtors.add(new Debtor(customerLoanAccountIdentifier, amount.toPlainString()));
    debtors.add(new Debtor(LOAN_FUNDS_SOURCE_ACCOUNT_IDENTIFIER, principal.toPlainString()));
    if (interestAccrued.compareTo(BigDecimal.ZERO) != 0)
      debtors.add(new Debtor(LOAN_INTEREST_ACCRUAL_ACCOUNT_IDENTIFIER, interestAccrued.toPlainString()));

    final Set<Creditor> creditors = new HashSet<>();
    creditors.add(new Creditor(TELLER_ONE_ACCOUNT_IDENTIFIER, amount.toPlainString()));
    creditors.add(new Creditor(LOANS_PAYABLE_ACCOUNT_IDENTIFIER, principal.toPlainString()));
    if (interestAccrued.compareTo(BigDecimal.ZERO) != 0)
      creditors.add(new Creditor(CONSUMER_LOAN_INTEREST_ACCOUNT_IDENTIFIER, interestAccrued.toPlainString()));

    verifyTransfer(debtors, creditors, Action.ACCEPT_PAYMENT);

    expectedCurrentBalance = expectedCurrentBalance.subtract(amount);
    interestAccrued = BigDecimal.ZERO;
  }

  private void step8Close() throws InterruptedException {
    logger.info("step8Close");

    checkStateTransfer(
        product.getIdentifier(),
        customerCase.getIdentifier(),
        Action.CLOSE,
        Collections.singletonList(assignEntryToTeller()),
        IndividualLoanEventConstants.CLOSE_INDIVIDUALLOAN_CASE,
        Case.State.CLOSED); //Close has to be done explicitly.
  }


  private Product createProduct() throws InterruptedException {
    final Product product = defineProductWithoutAccountAssignments();

    product.setAccountAssignments(defineAccountAssignments());

    bastet.api().createProduct(product);
    Assert.assertTrue(this.eventRecorder.wait(EventConstants.POST_PRODUCT, product.getIdentifier()));

    return product;
  }

  private Set<AccountAssignment> defineAccountAssignments() {
    final Set<AccountAssignment> accountAssignments = new HashSet<>();

    final AccountAssignment customerLoanLedgerAssignment = new AccountAssignment();
    customerLoanLedgerAssignment.setDesignator(CUSTOMER_LOAN);
    customerLoanLedgerAssignment.setLedgerIdentifier(CUSTOMER_LOAN_LEDGER_IDENTIFIER);
    accountAssignments.add(customerLoanLedgerAssignment);

    accountAssignments.add(new AccountAssignment(PENDING_DISBURSAL, PENDING_DISBURSAL_ACCOUNT_IDENTIFIER));
    accountAssignments.add(new AccountAssignment(PROCESSING_FEE_INCOME, PROCESSING_FEE_INCOME_ACCOUNT_IDENTIFIER));
    accountAssignments.add(new AccountAssignment(ORIGINATION_FEE_INCOME, LOAN_ORIGINATION_FEES_ACCOUNT_IDENTIFIER));
    accountAssignments.add(new AccountAssignment(DISBURSEMENT_FEE_INCOME, DISBURSEMENT_FEE_INCOME_ACCOUNT_IDENTIFIER));
    accountAssignments.add(new AccountAssignment(INTEREST_INCOME, CONSUMER_LOAN_INTEREST_ACCOUNT_IDENTIFIER));
    accountAssignments.add(new AccountAssignment(INTEREST_ACCRUAL, LOAN_INTEREST_ACCRUAL_ACCOUNT_IDENTIFIER));
    accountAssignments.add(new AccountAssignment(LOANS_PAYABLE, LOANS_PAYABLE_ACCOUNT_IDENTIFIER));
    accountAssignments.add(new AccountAssignment(LATE_FEE_INCOME, LATE_FEE_INCOME_ACCOUNT_IDENTIFIER));
    accountAssignments.add(new AccountAssignment(LATE_FEE_ACCRUAL, LATE_FEE_ACCRUAL_ACCOUNT_IDENTIFIER));
    accountAssignments.add(new AccountAssignment(ARREARS_ALLOWANCE, ARREARS_ALLOWANCE_ACCOUNT_IDENTIFIER));

    //accountAssignments.add(new AccountAssignment(ENTRY, ...));
    // Don't assign entry account in test since it usually will not be assigned IRL.
    accountAssignments.add(new AccountAssignment(LOAN_FUNDS_SOURCE, LOAN_FUNDS_SOURCE_ACCOUNT_IDENTIFIER));
    return accountAssignments;
  }

  private Product defineProductWithoutAccountAssignments() {
    final Product product = new Product();
    product.setIdentifier(bastet.getProcessEnvironment().generateUniqueIdentifer("agro"));
    product.setPatternPackage("io.mifos.individuallending.api.v1");

    product.setName("Agricultural Loan");
    product.setDescription("Loan for seeds or agricultural equipment");
    product.setTermRange(new TermRange(ChronoUnit.MONTHS, 12));
    product.setBalanceRange(new BalanceRange(BigDecimal.ZERO, new BigDecimal(10000)));
    product.setInterestBasis(InterestBasis.CURRENT_BALANCE);

    product.setCurrencyCode("XXX");
    product.setMinorCurrencyUnitDigits(MINOR_CURRENCY_UNIT_DIGITS);

    product.setAccountAssignments(Collections.emptySet());

    final ProductParameters productParameters = new ProductParameters();

    productParameters.setMoratoriums(Collections.emptyList());
    productParameters.setMaximumDispersalCount(5);

    final Gson gson = new Gson();
    product.setParameters(gson.toJson(productParameters));
    return product;
  }

  private void setFeeToFixedValue(final String productIdentifier,
                                  final String feeId,
                                  final BigDecimal amount) throws InterruptedException {
    final ChargeDefinition chargeDefinition
        = bastet.api().getChargeDefinition(productIdentifier, feeId);
    chargeDefinition.setChargeMethod(ChargeDefinition.ChargeMethod.FIXED);
    chargeDefinition.setAmount(amount);
    chargeDefinition.setProportionalTo(null);
    bastet.api().changeChargeDefinition(productIdentifier, feeId, chargeDefinition);
    Assert.assertTrue(this.eventRecorder.wait(EventConstants.PUT_CHARGE_DEFINITION,
        new ChargeDefinitionEvent(productIdentifier, feeId)));
  }

  private TaskDefinition createTaskDefinition(Product product) throws InterruptedException {
    final TaskDefinition taskDefinition = getTaskDefinition();
    bastet.api().createTaskDefinition(product.getIdentifier(), taskDefinition);
    Assert.assertTrue(this.eventRecorder.wait(EventConstants.POST_TASK_DEFINITION, new TaskDefinitionEvent(product.getIdentifier(), taskDefinition.getIdentifier())));
    return taskDefinition;
  }

  private TaskDefinition getTaskDefinition() {
    final TaskDefinition ret = new TaskDefinition();
    ret.setIdentifier(bastet.getProcessEnvironment().generateUniqueIdentifer("task"));
    ret.setDescription("But how do you feel about this?");
    ret.setName("feep");
    ret.setMandatory(true);
    ret.setActions(Collections.singleton(Action.APPROVE.name()));
    ret.setFourEyes(false);
    return ret;
  }

  private CaseParameters getCaseParameters()
  {
    final CaseParameters ret = new CaseParameters(bastet.getProcessEnvironment().generateUniqueIdentifer("fred"));

    ret.setCustomerIdentifier("alice");
    ret.setMaximumBalance(BigDecimal.valueOf(2000L));
    ret.setTermRange(new TermRange(ChronoUnit.MONTHS, 18));
    ret.setPaymentCycle(new PaymentCycle(ChronoUnit.MONTHS, 1, 1, null, null));

    final CreditWorthinessSnapshot customerCreditWorthinessSnapshot = new CreditWorthinessSnapshot();
    customerCreditWorthinessSnapshot.setForCustomer("alice");
    customerCreditWorthinessSnapshot.setDebts(Collections.singletonList(new CreditWorthinessFactor("some debt", BigDecimal.valueOf(300))));
    customerCreditWorthinessSnapshot.setAssets(Collections.singletonList(new CreditWorthinessFactor("some asset", BigDecimal.valueOf(500))));
    customerCreditWorthinessSnapshot.setIncomeSources(Collections.singletonList(new CreditWorthinessFactor("some income source", BigDecimal.valueOf(300))));

    final CreditWorthinessSnapshot cosignerCreditWorthinessSnapshot = new CreditWorthinessSnapshot();
    cosignerCreditWorthinessSnapshot.setForCustomer("seema");
    cosignerCreditWorthinessSnapshot.setDebts(Collections.emptyList());
    cosignerCreditWorthinessSnapshot.setAssets(Collections.singletonList(new CreditWorthinessFactor("a house", BigDecimal.valueOf(50000))));
    cosignerCreditWorthinessSnapshot.setIncomeSources(Collections.singletonList(new CreditWorthinessFactor("retirement", BigDecimal.valueOf(200))));

    final List<CreditWorthinessSnapshot> creditWorthinessSnapshots = new ArrayList<>();
    creditWorthinessSnapshots.add(customerCreditWorthinessSnapshot);
    creditWorthinessSnapshots.add(cosignerCreditWorthinessSnapshot);

    ret.setCreditWorthinessSnapshots(creditWorthinessSnapshots);

    return ret;
  }

  private Case createAdjustedCase(final String productIdentifier, final Consumer<Case> adjustment) throws InterruptedException {
    final Case caseInstance = getCase(productIdentifier);
    adjustment.accept(caseInstance);

    final ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    final Validator validator = factory.getValidator();
    final Set<ConstraintViolation<Case>> errors = validator.validate(caseInstance);
    Assert.assertEquals(0, errors.size());

    bastet.api().createCase(productIdentifier, caseInstance);
    Assert.assertTrue(this.eventRecorder.wait(EventConstants.POST_CASE,
        new CaseEvent(productIdentifier, caseInstance.getIdentifier())));

    return caseInstance;
  }

  private Case getCase(final String productIdentifier) {
    final Case ret = new Case();

    ret.setIdentifier(bastet.getProcessEnvironment().generateUniqueIdentifer("loan"));
    ret.setProductIdentifier(productIdentifier);


    final Set<AccountAssignment> accountAssignments = new HashSet<>();
    ret.setAccountAssignments(accountAssignments);
    ret.setCurrentState(Case.State.CREATED.name());

    final CaseParameters caseParameters = getCaseParameters();
    final Gson gson = new Gson();
    ret.setParameters(gson.toJson(caseParameters));

    return ret;
  }

  private AccountAssignment assignEntryToTeller() {
    final AccountAssignment entryAccountAssignment = new AccountAssignment();
    entryAccountAssignment.setDesignator(AccountDesignators.ENTRY);
    entryAccountAssignment.setAccountIdentifier(TELLER_ONE_ACCOUNT_IDENTIFIER);
    return entryAccountAssignment;
  }

  private void checkStateTransfer(final String productIdentifier,
                                  final String caseIdentifier,
                                  final Action action,
                                  final List<AccountAssignment> oneTimeAccountAssignments,
                                  final String event,
                                  final Case.State nextState) throws InterruptedException {
    checkStateTransfer(productIdentifier, caseIdentifier, action, oneTimeAccountAssignments, BigDecimal.ZERO, event, nextState);
  }

  private void checkStateTransfer(final String productIdentifier,
                                  final String caseIdentifier,
                                  final Action action,
                                  final List<AccountAssignment> oneTimeAccountAssignments,
                                  final BigDecimal paymentSize,
                                  final String event,
                                  final Case.State nextState) throws InterruptedException {
    final Command command = new Command();
    command.setOneTimeAccountAssignments(oneTimeAccountAssignments);
    command.setPaymentSize(paymentSize);
    bastet.api().executeCaseCommand(productIdentifier, caseIdentifier, action.name(), command);

    final IndividualLoanCommandEvent expectedEvent = new IndividualLoanCommandEvent(productIdentifier, caseIdentifier);
    Assert.assertTrue(expectedEvent.toString(), eventRecorder.wait(event, expectedEvent));

    final Case customerCase = bastet.api().getCase(productIdentifier, caseIdentifier);
    Assert.assertEquals(nextState.name(), customerCase.getCurrentState());
  }

  private void markTaskExecuted(final Product product,
                                final Case customerCase,
                                final TaskDefinition taskDefinition) throws InterruptedException {
    bastet.api().markTaskExecution(product.getIdentifier(), customerCase.getIdentifier(), taskDefinition.getIdentifier(), true);
    Assert.assertTrue(eventRecorder.wait(EventConstants.PUT_TASK_INSTANCE_EXECUTION, new TaskInstanceEvent(product.getIdentifier(), customerCase.getIdentifier(), taskDefinition.getIdentifier())));
  }

  private String verifyAccountCreation(final String ledgerIdentifier,
                                       final AccountType type) throws InterruptedException {
    final Function<String, Boolean> accountMatcher = (x) -> true;
    Assert.assertTrue(eventRecorder.waitForMatch(io.mifos.accounting.api.v1.EventConstants.POST_ACCOUNT, accountMatcher));

    final AccountPage lastAccountPageOfLedger = thoth.api().fetchAccountsOfLedger(
        ledgerIdentifier,
        0,
        1,
        "createdOn",
        Sort.Direction.DESC.name());
    Assert.assertEquals(1, lastAccountPageOfLedger.getAccounts().size());
    final Account lastAccount = lastAccountPageOfLedger.getAccounts().get(0);
    Assert.assertEquals(type.name(), lastAccount.getType());
    return lastAccount.getIdentifier();
  }


  private void verifyTransfer(final String fromAccountIdentifier,
                              final String toAccountIdentifier,
                              final BigDecimal amount,
                              final Action action) throws InterruptedException {
    final Set<Debtor> debtors = new HashSet<>();
    debtors.add(new Debtor(fromAccountIdentifier, amount.toPlainString()));

    final Set<Creditor> creditors = new HashSet<>();
    creditors.add(new Creditor(toAccountIdentifier, amount.toPlainString()));

    verifyTransfer(debtors, creditors, action);
  }

  private void verifyTransfer(final Set<Debtor> debtors,
                              final Set<Creditor> creditors,
                              final Action action) throws InterruptedException {
    final String matchPrefix = "portfolio." + product.getIdentifier() + "." + customerCase.getIdentifier() + "." + action.name();
    Assert.assertTrue( eventRecorder.waitForMatch(io.mifos.accounting.api.v1.EventConstants.RELEASE_JOURNAL_ENTRY, (String x) -> x.startsWith(matchPrefix)));
    TimeUnit.SECONDS.sleep(1);

    debtors.forEach(debtor ->
        checkAccountEntry(
            debtor.getAccountNumber(),
            BigDecimal.valueOf(Double.valueOf(debtor.getAmount())).setScale(MINOR_CURRENCY_UNIT_DIGITS, BigDecimal.ROUND_HALF_EVEN)));

    creditors.forEach(creditor ->
        checkAccountEntry(
            creditor.getAccountNumber(),
            BigDecimal.valueOf(Double.valueOf(creditor.getAmount())).setScale(MINOR_CURRENCY_UNIT_DIGITS, BigDecimal.ROUND_HALF_EVEN)));
  }

  private void checkAccountEntry(final String accountIdentifier, final BigDecimal amount) {
    final LocalDate today = LocalDate.now(Clock.systemUTC());
    final DateRange todayDateRange = new DateRange(today, today);
    final AccountEntryPage accountEntriesPage = thoth.api().fetchAccountEntries(
        accountIdentifier,
        todayDateRange.toString(),
        null,
        0,
        2,
        "transactionDate",
        Sort.Direction.DESC.name());
    Assert.assertTrue("There should be at least 1 account entry.", accountEntriesPage.getAccountEntries().size() >= 1);

    final List<BigDecimal> amounts = accountEntriesPage.getAccountEntries()
        .stream()
        .map(AccountEntry::getAmount)
        .map(BigDecimal::valueOf)
        .map(x -> x.setScale(MINOR_CURRENCY_UNIT_DIGITS, BigDecimal.ROUND_HALF_EVEN))
        .collect(Collectors.toList());
    Assert.assertTrue("No entries matched the amount " + amount +
        ", for the account " + accountIdentifier +
        ", for the loan and case " + product.getIdentifier() + "." + customerCase.getIdentifier() +
        ".  Amounts of the last two entries were " + amounts, amounts.contains(amount));
  }
}