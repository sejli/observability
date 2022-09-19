/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.observability.resthandler

import org.opensearch.client.node.NodeClient
import org.opensearch.commons.utils.logger
import org.opensearch.observability.ObservabilityPlugin
import org.opensearch.observability.ObservabilityPlugin.Companion.BASE_COLLABORATION_URI
import org.opensearch.observability.collaboration.action.CollaborationActions
import org.opensearch.observability.collaboration.action.CreateCollaborationObjectAction
import org.opensearch.observability.collaboration.action.CreateCollaborationObjectRequest
import org.opensearch.observability.collaboration.action.DeleteCollaborationObjectAction
import org.opensearch.observability.collaboration.action.DeleteCollaborationObjectRequest
import org.opensearch.observability.collaboration.action.GetCollaborationObjectAction
import org.opensearch.observability.collaboration.action.GetCollaborationObjectRequest
import org.opensearch.observability.collaboration.model.CollaborationObjectType
import org.opensearch.observability.index.CollaborationQueryHelper
import org.opensearch.observability.model.RestTag.COLLABORATION_ID_FIELD
import org.opensearch.observability.model.RestTag.COLLABORATION_ID_LIST_FIELD
import org.opensearch.observability.model.RestTag.FROM_INDEX_FIELD
import org.opensearch.observability.model.RestTag.MAX_ITEMS_FIELD
import org.opensearch.observability.model.RestTag.OBJECT_ID_LIST_FIELD
import org.opensearch.observability.model.RestTag.OBJECT_TYPE_FIELD
import org.opensearch.observability.model.RestTag.SORT_FIELD_FIELD
import org.opensearch.observability.model.RestTag.SORT_ORDER_FIELD
import org.opensearch.observability.settings.PluginSettings
import org.opensearch.observability.util.contentParserNextToken
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.DELETE
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.RestStatus
import org.opensearch.search.sort.SortOrder
import java.util.EnumSet

/**
 * Rest handler for observability object lifecycle management.
 * This handler uses [CollaborationActions].
 */
internal class CollaborationsRestHandler : BaseRestHandler() {
    companion object {
        private const val COLLABORATION_ACTION = "collaboration_actions"
        private const val COLLABORATION_URL = "$BASE_COLLABORATION_URI/collaborations"
        private val log by logger(CollaborationsRestHandler::class.java)
    }

    /**
     * {@inheritDoc}
     */
    override fun getName(): String {
        return COLLABORATION_ACTION
    }

    /**
     * {@inheritDoc}
     */
    override fun routes(): List<Route> {
        return listOf(
            /**
             * Creates a new collaboration
             * Request URL: POST COLLABORATION_URL
             * Request body: Ref [org.opensearch.observability.model.CreateObservabilityObjectRequest]
             * Response body: Ref [org.opensearch.observability.model.CreateObservabilityObjectResponse]
             */
            Route(POST, "$COLLABORATION_URL/{$COLLABORATION_ID_FIELD}"),
            Route(POST, COLLABORATION_URL),
            /**
             * Get a new collaboration
             * Request URL: GET COLLABORATION_URL/{objectId}
             * Request body: Ref [org.opensearch.observability.model.GetObservabilityObjectRequest]
             * Response body: Ref [org.opensearch.observability.model.GetObservabilityObjectResponse]
             */
            Route(GET, "$COLLABORATION_URL/{$COLLABORATION_ID_FIELD}"),
            Route(GET, COLLABORATION_URL),
            /**
             * Delete a collaboration object
             * Request URL: DELETE COLLABORATION_URL/{collaborationId}
             * Request body: Ref [org.opensearch.observability.model.DeleteObservabilityObjectRequest]
             * Response body: Ref [org.opensearch.observability.model.DeleteObservabilityObjectResponse]
             */
            Route(DELETE, "$COLLABORATION_URL/{$COLLABORATION_ID_FIELD}")
        )
    }

    /**
     * {@inheritDoc}
     */
    override fun responseParams(): Set<String> {
        return setOf(
            COLLABORATION_ID_FIELD
        )
    }

    private fun executePostCollaborationRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val collaborationId: String? = request.param(COLLABORATION_ID_FIELD)
        return RestChannelConsumer {
            client.execute(
                CreateCollaborationObjectAction.ACTION_TYPE,
                CreateCollaborationObjectRequest.parse(request.contentParserNextToken(), collaborationId),
                RestResponseToXContentListener(it)
            )
        }
    }

    private fun executeGetCollaborationRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val collaborationId: String? = request.param(COLLABORATION_ID_FIELD)
        val collaborationIdListString: String? = request.param(OBJECT_ID_LIST_FIELD)
        val collaborationIdList = getCollaborationIdSet(collaborationId, collaborationIdListString)
        val types: EnumSet<CollaborationObjectType> = getTypesSet(request.param(OBJECT_TYPE_FIELD))
        val sortField: String? = request.param(SORT_FIELD_FIELD)
        val sortOrderString: String? = request.param(SORT_ORDER_FIELD)
        val sortOrder: SortOrder? = if (sortOrderString == null) {
            null
        } else {
            SortOrder.fromString(sortOrderString)
        }
        val fromIndex = request.param(FROM_INDEX_FIELD)?.toIntOrNull() ?: 0
        val maxItems = request.param(MAX_ITEMS_FIELD)?.toIntOrNull() ?: PluginSettings.defaultItemsQueryCount
        val filterParams = request.params()
            .filter { CollaborationQueryHelper.FILTER_PARAMS.contains(it.key) }
            .map { Pair(it.key, request.param(it.key)) }
            .toMap()
        log.info(
            "${ObservabilityPlugin.LOG_PREFIX}:executeGetRequest idList:$collaborationIdList types:$types, from:$fromIndex, maxItems:$maxItems," +
                " sortField:$sortField, sortOrder=$sortOrder, filters=$filterParams"
        )
        return RestChannelConsumer {
            client.execute(
                GetCollaborationObjectAction.ACTION_TYPE,
                GetCollaborationObjectRequest(
                    collaborationIdList,
                    types,
                    fromIndex,
                    maxItems,
                    sortField,
                    sortOrder,
                    filterParams
                ),
                RestResponseToXContentListener(it)
            )
        }
    }

    private fun executeDeleteRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val collaborationId: String? = request.param(COLLABORATION_ID_FIELD)
        val collaborationIdSet: Set<String> =
            request.paramAsStringArray(COLLABORATION_ID_FIELD, arrayOf(collaborationId))
                .filter { s -> !s.isNullOrBlank() }
                .toSet()
        return RestChannelConsumer {
            if (collaborationIdSet.isEmpty()) {
                it.sendResponse(
                    BytesRestResponse(
                        RestStatus.BAD_REQUEST,
                        "Either $COLLABORATION_ID_FIELD or $COLLABORATION_ID_LIST_FIELD is required"
                    )
                )
            } else {
                client.execute(
                    DeleteCollaborationObjectAction.ACTION_TYPE,
                    DeleteCollaborationObjectRequest(collaborationIdSet),
                    RestResponseToXContentListener(it)
                )
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        return when (request.method()) {
            POST -> executePostCollaborationRequest(request, client)
            GET -> executeGetCollaborationRequest(request, client)
            DELETE -> executeDeleteRequest(request, client)
            else -> RestChannelConsumer {
                it.sendResponse(BytesRestResponse(RestStatus.METHOD_NOT_ALLOWED, "${request.method()} is not allowed"))
            }
        }
    }

    private fun getCollaborationIdSet(collaborationId: String?, collaborationIdList: String?): Set<String> {
        var retIds: Set<String> = setOf()
        if (collaborationId != null) {
            retIds = setOf(collaborationId)
        }
        if (collaborationIdList != null) {
            retIds = collaborationIdList.split(",").union(retIds)
        }
        return retIds
    }

    private fun getTypesSet(typesString: String?): EnumSet<CollaborationObjectType> {
        var types: EnumSet<CollaborationObjectType> = EnumSet.noneOf(CollaborationObjectType::class.java)
        typesString?.split(",")?.forEach { types.add(CollaborationObjectType.fromTagOrDefault(it)) }
        return types
    }
}
