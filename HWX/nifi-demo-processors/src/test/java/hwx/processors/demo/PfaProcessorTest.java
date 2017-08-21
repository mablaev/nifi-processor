/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hwx.processors.demo;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;


public class PfaProcessorTest {

    private TestRunner testRunner;

    private static final String testJsonInput = "[ {\n" +
            "  \"order_date\" : 20170406,\n" +
            "  \"security_identifier\" : \"FR0011052257\",\n" +
            "  \"security_identifier_type\" : \"ISIN\",\n" +
            "  \"asset_class\" : \"Equities\",\n" +
            "  \"investor_type\" : \"P\",\n" +
            "  \"account\" : null,\n" +
            "  \"transaction_type\" : \"REG\",\n" +
            "  \"parent_directed_status\" : \"N\",\n" +
            "  \"strategy_intent\" : \"P\",\n" +
            "  \"strategy_name\" : \"Worked\",\n" +
            "  \"order_type\" : \"LIM\",\n" +
            "  \"client_order_id\" : null,\n" +
            "  \"routed_order_id\" : \"3000000015\",\n" +
            "  \"route_venue\" : \"529900ZJEIQ4T5Z8KE50\",\n" +
            "  \"route_venue_type\" : \"BRKR\",\n" +
            "  \"routed_order_size\" : \"1166\",\n" +
            "  \"order_executed_qty\" : \"1166\",\n" +
            "  \"pm_manager_code\" : \"ABC65\",\n" +
            "  \"region\" : \"OTH\"\n" +
            "} ][ {\n" +
            "  \"trade_date\" : 20170406,\n" +
            "  \"routed_order_id\" : \"3000000015\",\n" +
            "  \"order_date\" : 20170406,\n" +
            "  \"execution_id\" : \"00266793238TRPA1\",\n" +
            "  \"last_market\" : \"XPAR\",\n" +
            "  \"executed_qty\" : 56,\n" +
            "  \"liquidity_type\" : null\n" +
            "}, {\n" +
            "  \"trade_date\" : 20170406,\n" +
            "  \"routed_order_id\" : \"3000000015\",\n" +
            "  \"order_date\" : 20170406,\n" +
            "  \"execution_id\" : \"00266797153TRPA1\",\n" +
            "  \"last_market\" : \"XPAR\",\n" +
            "  \"executed_qty\" : 25,\n" +
            "  \"liquidity_type\" : null\n" +
            "} ]\n";

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(PfaProcessor.class);
    }

    @Test
    public void testProcessor() throws IOException {
        // Content to be mock a json file
        InputStream content = new ByteArrayInputStream(testJsonInput.getBytes());

        // Generate a test runner to mock a processor in a flow
        TestRunner runner = TestRunners.newTestRunner(new PfaProcessor());

        runner.setProperty(PfaProcessor.ORDER_JSON_PATH, "$.security_identifier");
        runner.setProperty(PfaProcessor.EXECUTION_JSON_PATH, "$.execution_id");

        // Add the content to the runner
        runner.enqueue(content);

        runner.run(1);

        // All results were processed with out failure
        runner.assertQueueEmpty();

        // If you need to read or do aditional tests on results you can access the content
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(PfaProcessor.SUCCESS_RTS_28);
        Assert.assertTrue("1 match", results.size() == 1);
        MockFlowFile result = results.get(0);
        String resultValue = new String(runner.getContentAsByteArray(result));
        System.out.println("Match: " + new String(runner.getContentAsByteArray(result)));

        // Test attributes and content
        result.assertAttributeEquals(PfaProcessor.MATCH_ATTR, "test");
        result.assertContentEquals("test");
    }

}
