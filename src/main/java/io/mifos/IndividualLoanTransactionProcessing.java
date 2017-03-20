/*
 * Copyright ${year} ${name}.
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

import io.mifos.core.api.config.EnableApiFactory;
import io.mifos.core.api.context.AutoUserContext;
import io.mifos.core.api.util.ApiFactory;
import io.mifos.core.test.fixture.cassandra.CassandraInitializer;
import io.mifos.core.test.fixture.mariadb.MariaDBInitializer;
import io.mifos.core.test.servicestarter.EurekaForTest;
import io.mifos.core.test.servicestarter.InitializedMicroservice;
import io.mifos.core.test.servicestarter.IntegrationTestEnvironment;
import io.mifos.portfolio.api.v1.client.PortfolioManager;
import io.mifos.accounting.api.v1.client.LedgerManager;
import io.mifos.portfolio.api.v1.domain.Pattern;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;


@RunWith(SpringRunner.class)
@SpringBootTest()
public class IndividualLoanTransactionProcessing {
  @Configuration
  @EnableApiFactory
  public static class TestConfiguration {
    public TestConfiguration() {
      super();
    }

    @Bean()
    public Logger logger() {
      return LoggerFactory.getLogger("test-logger");
    }
  }


  private final static EurekaForTest eurekaForTest = new EurekaForTest();
  private final static CassandraInitializer cassandraInitializer = new CassandraInitializer();
  private final static MariaDBInitializer mariaDBInitializer = new MariaDBInitializer();
  private final static IntegrationTestEnvironment integrationTestEnvironment = new IntegrationTestEnvironment(cassandraInitializer, mariaDBInitializer);

  private final static InitializedMicroservice<LedgerManager> thoth = new InitializedMicroservice<>(LedgerManager.class, "accounting", "0.1.0-BUILD-SNAPSHOT", integrationTestEnvironment);
  private final static InitializedMicroservice<PortfolioManager> bastet= new InitializedMicroservice<>(PortfolioManager.class, "portfolio", "0.1.0-BUILD-SNAPSHOT", integrationTestEnvironment);

  @ClassRule
  public static TestRule orderedRules = RuleChain
          .outerRule(eurekaForTest)
          .around(cassandraInitializer)
          .around(mariaDBInitializer)
          .around(integrationTestEnvironment)
          .around(thoth)
          .around(bastet);

  @Autowired
  private ApiFactory apiFactory;

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
  public void test() {
    try (final AutoUserContext ignored = integrationTestEnvironment.createAutoUserContext("blah")) {
      final List<Pattern> patterns = bastet.api().findAllPatterns();
      Assert.assertTrue(patterns != null);
    }
  }
}