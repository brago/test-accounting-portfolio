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
import io.mifos.accounting.importer.AccountImporter;
import io.mifos.accounting.importer.LedgerImporter;
import io.mifos.core.api.config.EnableApiFactory;
import io.mifos.core.api.context.AutoUserContext;
import io.mifos.core.api.util.ApiFactory;
import io.mifos.core.test.env.ExtraProperties;
import io.mifos.core.test.fixture.cassandra.CassandraInitializer;
import io.mifos.core.test.fixture.mariadb.MariaDBInitializer;
import io.mifos.core.test.listener.EventRecorder;
import io.mifos.core.test.servicestarter.ActiveMQForTest;
import io.mifos.core.test.servicestarter.EurekaForTest;
import io.mifos.core.test.servicestarter.InitializedMicroservice;
import io.mifos.core.test.servicestarter.IntegrationTestEnvironment;
import io.mifos.individuallending.api.v1.domain.product.AccountDesignators;
import io.mifos.individuallending.api.v1.domain.product.ProductParameters;
import io.mifos.portfolio.api.v1.client.PortfolioManager;
import io.mifos.portfolio.api.v1.client.ProductDefinitionIncomplete;
import io.mifos.portfolio.api.v1.domain.*;
import io.mifos.portfolio.api.v1.events.EventConstants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
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
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.mifos.portfolio.api.v1.events.EventConstants.PUT_PRODUCT;
import static java.math.BigDecimal.ROUND_HALF_EVEN;


@SuppressWarnings("SpringAutowiredFieldsWarningInspection")
@RunWith(SpringRunner.class)
@SpringBootTest()
public class IndividualLoanTransactionProcessing {
  private static final String SCHEDULER_USER_NAME = "imhotep";
  private static final String TEST_LOGGER = "test-logger";

  private static final String INCOME_LEDGER_IDENTIFIER = "1000";
  private static final String LOAN_INCOME_LEDGER_IDENTIFIER = "1100";
  private static final String FEES_AND_CHARGES_LEDGER_IDENTIFIER = "1300";
  private static final String ASSET_LEDGER_IDENTIFIER = "7000";
  private static final String CASH_LEDGER_IDENTIFIER = "7300";
  private static final String PENDING_DISBURSAL_LEDGER_IDENTIFIER = "7320";
  private static final String CUSTOMER_LOAN_LEDGER_IDENTIFIER = "7353";
  private static final String ACCRUED_INCOME_LEDGER_IDENTIFIER = "7800";

  private static final String LOAN_FUNDS_SOURCE_ACCOUNT_IDENTIFIER = "7310";
  private static final String LOAN_ORIGINATION_FEES_ACCOUNT_IDENTIFIER = "1310";
  private static final String PROCESSING_FEE_INCOME_ACCOUNT_IDENTIFIER = "1312";
  private static final String DISBURSEMENT_FEE_INCOME_ACCOUNT_IDENTIFIER = "1313";
  private static final String TELLER_ONE_ACCOUNT_IDENTIFIER = "7352";
  private static final String LOAN_INTEREST_ACCRUAL_ACCOUNT_IDENTIFIER = "7810";
  private static final String CONSUMER_LOAN_INTEREST_ACCOUNT_IDENTIFIER = "1103";
  private static final String LOANS_PAYABLE_ACCOUNT_IDENTIFIER ="missingInChartOfAccounts";
  private static final String LATE_FEE_INCOME_ACCOUNT_IDENTIFIER = "001-008"; //TODO: ??
  private static final String LATE_FEE_ACCRUAL_ACCOUNT_IDENTIFIER = "001-009"; //TODO: ??
  private static final String ARREARS_ALLOWANCE_ACCOUNT_IDENTIFIER = "001-010"; //TODO: ??

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

  public IndividualLoanTransactionProcessing() {
    super();
  }

  @Before
  public void before()
  {
    bastet.setApiFactory(apiFactory);
    thoth.setApiFactory(apiFactory);
  }

  @Test
  public void test() throws InterruptedException {
    try (final AutoUserContext ignored = integrationTestEnvironment.createAutoUserContext("blah")) {

      try {
        final LedgerImporter ledgerImporter = new LedgerImporter(thoth.api(), logger);
        final URL ledgersUrl = ClassLoader.getSystemResource("standardChartOfAccounts/ledgers.csv");
        Assert.assertNotNull(ledgersUrl);
        ledgerImporter.importCSV(ledgersUrl);

        final AccountImporter accountImporter = new AccountImporter(thoth.api(), logger);
        final URL accountsUrl = ClassLoader.getSystemResource("standardChartOfAccounts/accounts.csv");
        Assert.assertNotNull(accountsUrl);
        accountImporter.importCSV(accountsUrl);
      } catch (IOException e) {
        Assert.fail("Failed to import chart of accounts.");
      }

      final List<Pattern> patterns = bastet.api().getAllPatterns();
      Assert.assertTrue(patterns != null);
      Assert.assertTrue(patterns.size() >= 1);
      Assert.assertTrue(patterns.get(0) != null);
      Assert.assertEquals(patterns.get(0).getParameterPackage(), "io.mifos.individuallending.api.v1");

      final Product product = defineProductWithoutAccountAssignments(
              patterns.get(0).getParameterPackage(),
              bastet.getProcessEnvironment().generateUniqueIdentifer("agro"));

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

      final Set<AccountAssignment> accountAssignments = new HashSet<>();
      accountAssignments.add(new AccountAssignment(AccountDesignators.PENDING_DISBURSAL, PENDING_DISBURSAL_LEDGER_IDENTIFIER));
      accountAssignments.add(new AccountAssignment(AccountDesignators.PROCESSING_FEE_INCOME, PROCESSING_FEE_INCOME_ACCOUNT_IDENTIFIER));
      accountAssignments.add(new AccountAssignment(AccountDesignators.ORIGINATION_FEE_INCOME, LOAN_ORIGINATION_FEES_ACCOUNT_IDENTIFIER));
      accountAssignments.add(new AccountAssignment(AccountDesignators.DISBURSEMENT_FEE_INCOME, DISBURSEMENT_FEE_INCOME_ACCOUNT_IDENTIFIER));
      accountAssignments.add(new AccountAssignment(AccountDesignators.INTEREST_INCOME, CONSUMER_LOAN_INTEREST_ACCOUNT_IDENTIFIER));
      accountAssignments.add(new AccountAssignment(AccountDesignators.INTEREST_ACCRUAL, LOAN_INTEREST_ACCRUAL_ACCOUNT_IDENTIFIER));
      accountAssignments.add(new AccountAssignment(AccountDesignators.LOANS_PAYABLE, LOANS_PAYABLE_ACCOUNT_IDENTIFIER));
      accountAssignments.add(new AccountAssignment(AccountDesignators.LATE_FEE_INCOME, LATE_FEE_INCOME_ACCOUNT_IDENTIFIER));
      accountAssignments.add(new AccountAssignment(AccountDesignators.LATE_FEE_ACCRUAL, LATE_FEE_ACCRUAL_ACCOUNT_IDENTIFIER));
      accountAssignments.add(new AccountAssignment(AccountDesignators.ARREARS_ALLOWANCE, ARREARS_ALLOWANCE_ACCOUNT_IDENTIFIER));
      accountAssignments.add(new AccountAssignment(AccountDesignators.LOAN_FUNDS_SOURCE, LOAN_FUNDS_SOURCE_ACCOUNT_IDENTIFIER));
      accountAssignments.add(new AccountAssignment(AccountDesignators.CUSTOMER_LOAN, CUSTOMER_LOAN_LEDGER_IDENTIFIER));
      // Don't assign entry account in test since it usually will not be assigned IRL.
      changedProduct.setAccountAssignments(accountAssignments);

      bastet.api().changeProduct(changedProduct.getIdentifier(), changedProduct);
      Assert.assertTrue(this.eventRecorder.wait(PUT_PRODUCT, changedProduct.getIdentifier()));
    }
  }

  private Product defineProductWithoutAccountAssignments(final String patternPackage, final String identifier) {
    final Product product = new Product();
    product.setIdentifier(identifier);
    product.setPatternPackage(patternPackage);

    product.setName("Agricultural Loan");
    product.setDescription("Loan for seeds or agricultural equipment");
    product.setTermRange(new TermRange(ChronoUnit.MONTHS, 12));
    product.setBalanceRange(new BalanceRange(fixScale(BigDecimal.ZERO), fixScale(new BigDecimal(10000))));
    product.setInterestBasis(InterestBasis.CURRENT_BALANCE);

    product.setCurrencyCode("XXX");
    product.setMinorCurrencyUnitDigits(2);

    product.setAccountAssignments(Collections.emptySet());

    final ProductParameters productParameters = new ProductParameters();

    productParameters.setMoratoriums(Collections.emptyList());
    productParameters.setMaximumDispersalCount(5);

    final Gson gson = new Gson();
    product.setParameters(gson.toJson(productParameters));
    return product;
  }

  static public BigDecimal fixScale(final BigDecimal bigDecimal)
  {
    return bigDecimal.setScale(4, ROUND_HALF_EVEN);
  }
}