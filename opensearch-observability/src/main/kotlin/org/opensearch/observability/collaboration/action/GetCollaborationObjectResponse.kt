/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.observability.collaboration.action

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.observability.collaboration.model.CollaborationObjectSearchResult
import org.opensearch.observability.model.BaseResponse
import org.opensearch.observability.model.RestTag
import java.io.IOException

/**
 * Action Response for getting ObservabilityObject.
 */
internal class GetCollaborationObjectResponse : BaseResponse {
    val searchResult: CollaborationObjectSearchResult
    private val filterSensitiveInfo: Boolean

    companion object {

        /**
         * reader to create instance of class from writable.
         */
        val reader = Writeable.Reader { GetCollaborationObjectResponse(it) }

        /**
         * Creator used in REST communication.
         * @param parser XContentParser to deserialize data from.
         */
        @JvmStatic
        @Throws(IOException::class)
        fun parse(parser: XContentParser): GetCollaborationObjectResponse {
            return GetCollaborationObjectResponse(CollaborationObjectSearchResult(parser), false)
        }
    }

    /**
     * constructor for creating the class
     * @param searchResult the ObservabilityObject list
     */
    constructor(searchResult: CollaborationObjectSearchResult, filterSensitiveInfo: Boolean) {
        this.searchResult = searchResult
        this.filterSensitiveInfo = filterSensitiveInfo
    }

    /**
     * {@inheritDoc}
     */
    @Throws(IOException::class)
    constructor(input: StreamInput) : super(input) {
        searchResult = CollaborationObjectSearchResult(input)
        filterSensitiveInfo = input.readBoolean()
    }

    /**
     * {@inheritDoc}
     */
    @Throws(IOException::class)
    override fun writeTo(output: StreamOutput) {
        searchResult.writeTo(output)
        output.writeBoolean(filterSensitiveInfo)
    }

    /**
     * {@inheritDoc}
     */
    override fun toXContent(builder: XContentBuilder?, params: ToXContent.Params?): XContentBuilder {
        val xContentParams = if (filterSensitiveInfo) {
            RestTag.FILTERED_REST_OUTPUT_PARAMS
        } else {
            RestTag.REST_OUTPUT_PARAMS
        }
        return searchResult.toXContent(builder, xContentParams)
    }
}
