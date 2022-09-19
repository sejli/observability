/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.observability.collaboration.action

import org.opensearch.action.ActionType
import org.opensearch.action.support.ActionFilters
import org.opensearch.client.Client
import org.opensearch.common.inject.Inject
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.commons.authuser.User
import org.opensearch.observability.action.PluginBaseAction
import org.opensearch.transport.TransportService

/**
 * Get CollaborationObject transport action
 */
internal class GetCollaborationObjectAction @Inject constructor(
    transportService: TransportService,
    client: Client,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry
) : PluginBaseAction<GetCollaborationObjectRequest, GetCollaborationObjectResponse>(
    NAME,
    transportService,
    client,
    actionFilters,
    ::GetCollaborationObjectRequest
) {
    companion object {
        private const val NAME = "cluster:admin/opensearch/observability/collaborations/get"
        internal val ACTION_TYPE = ActionType(NAME, ::GetCollaborationObjectResponse)
    }

    /**
     * {@inheritDoc}
     */
    override fun executeRequest(request: GetCollaborationObjectRequest, user: User?): GetCollaborationObjectResponse {
        return CollaborationActions.get(request, user)
    }
}
