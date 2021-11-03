/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package org.opensearch.observability

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import org.junit.Assert
import java.time.Instant
import kotlin.test.assertTrue

private const val DEFAULT_TIME_ACCURACY_SEC = 5L

fun constructNotebookRequest(name: String = "test notebook"): String {
    return """
        {
            "notebook":{
                "dateCreated" : "2020-12-11T20:51:15.509Z",
                "name" : "$name",
                "dateModified" : "2020-12-11T21:04:55.336Z",
                "backend" : "Default",
                "paragraphs" : [
                    {
                        "output" : [
                            {
                                "result" : "# This is a markdown paragraph",
                                "outputType" : "MARKDOWN",
                                "execution_time" : "0s"
                            }
                        ],
                        "input" : {
                            "inputText" : "# This is a markdown paragraph",
                            "inputType" : "MARKDOWN"
                        },
                        "dateCreated" : "2020-12-11T21:04:39.997Z",
                        "dateModified" : "2020-12-11T21:04:48.207Z",
                        "id" : "paragraph_61e96a10-af19-4c7d-ae4e-d2e449c65dff"
                    }
                ]
            }
        }
    """.trimIndent()
}

fun constructSavedQueryRequest(name: String = "test saved query"): String {
    return """
        {
            "savedQuery": {
                "query": "search source=opensearch_dashboards_sample_data_logs | where utc_time > timestamp('2021-07-01 00:00:00') and utc_time < timestamp('2021-07-02 00:00:00')",
                "selected_date_range": {
                    "start": "now/15m",
                    "end": "now",
                    "text": "utc_time > timestamp('2021-07-01 00:00:00') and utc_time < timestamp('2021-07-02 00:00:00')"
                },
                "selected_timestamp": {
                    "name": "utc_time",
                    "type": "timestamp"
                },
                "selected_fields": {
                    "text": "| fields clientip, bytes, memory, host",
                    "tokens": [
                        { "name": "bytes", "type": "long" },
                        { "name": "clientip", "type": "ip" }
                    ]
                },
                "name": "$name",
                "description": "some descriptions related to this query"
            }
        }
    """.trimIndent()
}

fun constructSavedVisualizationRequest(name: String = "test saved visualization"): String {
    return """
        {
            "savedVisualization": {
                "query": "search source=opensearch_dashboards_sample_data_logs | where utc_time > timestamp('2021-07-01 00:00:00') and utc_time < timestamp('2021-07-02 00:00:00')",
                "selected_date_range": {
                    "start": "now/15m",
                    "end": "now",
                    "text": "utc_time > timestamp('2021-07-01 00:00:00') and utc_time < timestamp('2021-07-02 00:00:00')"
                },
                "selected_timestamp": {
                    "name": "utc_time",
                    "type": "timestamp"
                },
                "selected_fields": {
                    "text": "| fields clientip, bytes, memory, host",
                    "tokens": [
                        { "name": "bytes", "type": "long" },
                        { "name": "clientip", "type": "ip" }
                    ]
                },
                "type": "bar",
                "name": "$name",
                "description": "some descriptions related to this query"
            }
        }
    """.trimIndent()
}

fun constructOperationalPanelRequest(name: String = "test operational panel"): String {
    return """
        {
            "operationalPanel": {
                "name": "$name",
                "dateCreated": "2021-07-19T21:01:14.871Z",
                "dateModified": "2021-07-19T21:01:14.871Z",
                "visualizations": [
                    {
                        "id": "panelViz_7ba28e34-6fd8-489d-9b9f-1f83e006fb17",
                        "savedVisualizationId": "oyuecXwBYVazWqOOde0o",
                        "x": 0,
                        "y": 0,
                        "w": 10,
                        "h": 10
                    },
                    {
                        "id": "panelViz_7ba28e34-6fd8-489d-9b9f-165fdv6wd611",
                        "savedVisualizationId": "oiuccXwBYVazWqOO1e06",
                        "x": 20,
                        "y": 20,
                        "w": 30,
                        "h": 20
                    }
                ],
                "timeRange": {
                    "to": "now",
                    "from": "now-1d"
                },
                "queryFilter": {
                    "query": "| where Carrier='OpenSearch-Air'",
                    "language": "ppl"
                }
            }
        }
    """.trimIndent()
}

fun constructTimestampRequest(name: String = "index-name"): String {
    return """
        {
            "objectId": "$name",
            "timestamp": {
                "name": "$name",
                "index": "opensearch_dashboards_sample_data_logs",
                "type": "timestamp",
                "dsl_type": "date"
            }
        }
    """.trimIndent()
}

fun jsonify(text: String): JsonObject {
    return JsonParser.parseString(text).asJsonObject
}

fun validateTimeNearRefTime(time: Instant, refTime: Instant, accuracySeconds: Long) {
    assertTrue(time.plusSeconds(accuracySeconds).isAfter(refTime), "$time + $accuracySeconds > $refTime")
    assertTrue(time.minusSeconds(accuracySeconds).isBefore(refTime), "$time - $accuracySeconds < $refTime")
}

fun validateTimeRecency(time: Instant, accuracySeconds: Long = DEFAULT_TIME_ACCURACY_SEC) {
    validateTimeNearRefTime(time, Instant.now(), accuracySeconds)
}

fun validateErrorResponse(response: JsonObject, statusCode: Int, errorType: String = "status_exception") {
    Assert.assertNotNull("Error response content should be generated", response)
    val status = response.get("status").asInt
    val error = response.get("error").asJsonObject
    val rootCause = error.get("root_cause").asJsonArray
    val type = error.get("type").asString
    val reason = error.get("reason").asString
    Assert.assertEquals(statusCode, status)
    Assert.assertEquals(errorType, type)
    Assert.assertNotNull(reason)
    Assert.assertNotNull(rootCause)
    Assert.assertTrue(rootCause.size() > 0)
}