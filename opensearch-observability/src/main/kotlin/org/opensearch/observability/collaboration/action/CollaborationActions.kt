/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.observability.collaboration.action

import org.opensearch.OpenSearchStatusException
import org.opensearch.commons.authuser.User
import org.opensearch.observability.ObservabilityPlugin
import org.opensearch.observability.collaboration.model.CollaborationObjectDoc
import org.opensearch.observability.collaboration.model.CollaborationObjectSearchResult
import org.opensearch.observability.index.CollaborationIndex
import org.opensearch.observability.security.UserAccessManager
import org.opensearch.observability.util.logger
import org.opensearch.rest.RestStatus
import java.time.Instant

/**
 * CollaborationObject index operation actions.
 */
internal object CollaborationActions {

    private val log by logger(CollaborationActions::class.java)

    /**
     * Create new CollaborationObject
     * @param request [CreateCollaborationObjectRequest] object
     * @return [CreateCollaborationObjectResponse]
     */
    fun create(request: CreateCollaborationObjectRequest, user: User?): CreateCollaborationObjectResponse {
        log.info("${ObservabilityPlugin.LOG_PREFIX}:CollaborationObject-create")
        UserAccessManager.validateUser(user)
        val currentTime = Instant.now()
        val objectDoc = CollaborationObjectDoc(
            "ignore",
            currentTime,
            currentTime,
            UserAccessManager.getUserTenant(user),
            UserAccessManager.getAllAccessInfo(user),
            request.type,
            request.objectData
        )
        val docId = CollaborationIndex.createCollaborationObject(objectDoc, request.collaborationId)
        docId ?: throw OpenSearchStatusException(
            "CollaborationObject creation failed",
            RestStatus.INTERNAL_SERVER_ERROR
        )
        return CreateCollaborationObjectResponse(docId)
    }

    /**
     * Get ObservabilityObject info
     * @param request [GetObservabilityObjectRequest] object
     * @return [GetObservabilityObjectResponse]
     */
    fun get(request: GetCollaborationObjectRequest, user: User?): GetCollaborationObjectResponse {
        log.info("${ObservabilityPlugin.LOG_PREFIX}: CollaborationObject-get ${request.objectIds}")
        UserAccessManager.validateUser(user)
        return when (request.objectIds.size) {
            0 -> getAll(request, user)
            1 -> info(request.objectIds.first(), user)
            else -> info(request.objectIds, user)
        }
    }

    /**
     * Get ObservabilityObject info
     * @param objectId object id
     * @param user the user info object
     * @return [GetObservabilityObjectResponse]
     */
    private fun info(collaborationId: String, user: User?): GetCollaborationObjectResponse {
        log.info("${ObservabilityPlugin.LOG_PREFIX}:CollaborationObject-get $collaborationId")
        val collaborationObjectDocInfo = CollaborationIndex.getCollaborationObject(collaborationId)
        collaborationObjectDocInfo
            ?: run {
                throw OpenSearchStatusException(
                    "CollaborationObject $collaborationId not found",
                    RestStatus.NOT_FOUND
                )
            }
        val currentDoc = collaborationObjectDocInfo.collaborationObjectDoc
        if (!UserAccessManager.doesUserHasAccess(user, currentDoc.tenant, currentDoc.access)) {
            throw OpenSearchStatusException(
                "Permission denied for CollaborationObject $collaborationId",
                RestStatus.FORBIDDEN
            )
        }
        val docInfo = CollaborationObjectDoc(
            collaborationId,
            currentDoc.updatedTime,
            currentDoc.createdTime,
            currentDoc.tenant,
            currentDoc.access,
            currentDoc.type,
            currentDoc.objectData
        )
        return GetCollaborationObjectResponse(
            CollaborationObjectSearchResult(docInfo),
            UserAccessManager.hasAllInfoAccess(user)
        )
    }

    private fun info(collaborationIds: Set<String>, user: User?): GetCollaborationObjectResponse {
        log.info("${ObservabilityPlugin.LOG_PREFIX}: CollaborationObject-info $collaborationIds")
        val objectDocs = CollaborationIndex.getCollaborationObjects(collaborationIds)
        if (objectDocs.size != collaborationIds.size) {
            val mutableSet = collaborationIds.toMutableSet()
            objectDocs.forEach { mutableSet.remove(it.id) }
            throw OpenSearchStatusException(
                "CollaborationObject $mutableSet not found",
                RestStatus.NOT_FOUND
            )
        }
        objectDocs.forEach {
            val currentDoc = it.collaborationObjectDoc
            if (!UserAccessManager.doesUserHasAccess(user, currentDoc.tenant, currentDoc.access)) {
                throw OpenSearchStatusException(
                    "Permission denied for CollaborationObject ${it.id}",
                    RestStatus.FORBIDDEN
                )
            }
        }
        val configSearchResult = objectDocs.map {
            CollaborationObjectDoc(
                it.id!!,
                it.collaborationObjectDoc.updatedTime,
                it.collaborationObjectDoc.createdTime,
                it.collaborationObjectDoc.tenant,
                it.collaborationObjectDoc.access,
                it.collaborationObjectDoc.type,
                it.collaborationObjectDoc.objectData
            )
        }
        return GetCollaborationObjectResponse(
            CollaborationObjectSearchResult(configSearchResult),
            UserAccessManager.hasAllInfoAccess(user)
        )
    }

    /**
     * Get all ObservabilityObject matching the criteria
     * @param request [GetObservabilityObjectRequest] object
     * @param user the user info object
     * @return [GetObservabilityObjectResponse]
     */
    private fun getAll(request: GetCollaborationObjectRequest, user: User?): GetCollaborationObjectResponse {
        log.info("${ObservabilityPlugin.LOG_PREFIX}: CollaborationObject-getAll")
        val searchResult = CollaborationIndex.getAllCollaborationObjects(
            UserAccessManager.getUserTenant(user),
            UserAccessManager.getSearchAccessInfo(user),
            request
        )
        return GetCollaborationObjectResponse(searchResult, UserAccessManager.hasAllInfoAccess(user))
    }

    /**
     * Delete CollaborationObject
     * @param request [DeleteCollaborationObjectRequest] object
     * @param user the user info object
     * @return [DeleteCollaborationObjectResponse]
     */
    fun delete(request: DeleteCollaborationObjectRequest, user: User?): DeleteCollaborationObjectResponse {
        log.info("${ObservabilityPlugin.LOG_PREFIX}:CollaborationObject-delete ${request.collaborationIds}")
        return if (request.collaborationIds.size == 1) {
            delete(request.collaborationIds.first(), user)
        } else {
            delete(request.collaborationIds, user)
        }
    }

    /**
     * Delete by collaboration id
     *
     * @param collaborationId
     * @param user
     * @return [DeleteCollaborationObjectResponse]
     */
    private fun delete(collaborationId: String, user: User?): DeleteCollaborationObjectResponse {
        log.info("${ObservabilityPlugin.LOG_PREFIX}:CollaborationObject-delete $collaborationId")
        UserAccessManager.validateUser(user)
        val collaborationObjectDocInfo = CollaborationIndex.getCollaborationObject(collaborationId)
        collaborationObjectDocInfo
            ?: run {
                throw OpenSearchStatusException(
                    "CollaborationObject $collaborationId not found",
                    RestStatus.NOT_FOUND
                )
            }

        val currentDoc = collaborationObjectDocInfo.collaborationObjectDoc
        if (!UserAccessManager.doesUserHasAccess(user, currentDoc.tenant, currentDoc.access)) {
            throw OpenSearchStatusException(
                "Permission denied for CollaborationObject $collaborationId",
                RestStatus.FORBIDDEN
            )
        }
        if (!CollaborationIndex.deleteCollaborationObject(collaborationId)) {
            throw OpenSearchStatusException(
                "CollaborationObject $collaborationId delete failed",
                RestStatus.REQUEST_TIMEOUT
            )
        }
        return DeleteCollaborationObjectResponse(mapOf(Pair(collaborationId, RestStatus.OK)))
    }

    /**
     * Delete CollaborationObjects
     * @param collaborationIds CollaborationObject ids
     * @param user the user info object
     * @return [DeleteCollaborationObjectResponse]
     */
    private fun delete(collaborationIds: Set<String>, user: User?): DeleteCollaborationObjectResponse {
        log.info("${ObservabilityPlugin.LOG_PREFIX}:CollaborationObject-delete $collaborationIds")
        UserAccessManager.validateUser(user)
        val configDocs = CollaborationIndex.getCollaborationObjects(collaborationIds)
        if (configDocs.size != collaborationIds.size) {
            val mutableSet = collaborationIds.toMutableSet()
            configDocs.forEach { mutableSet.remove(it.id) }
            throw OpenSearchStatusException(
                "CollaborationObject $mutableSet not found",
                RestStatus.NOT_FOUND
            )
        }
        configDocs.forEach {
            val currentDoc = it.collaborationObjectDoc
            if (!UserAccessManager.doesUserHasAccess(user, currentDoc.tenant, currentDoc.access)) {
                throw OpenSearchStatusException(
                    "Permission denied for CollaborationObject ${it.id}",
                    RestStatus.FORBIDDEN
                )
            }
        }
        val deleteStatus = CollaborationIndex.deleteCollaborationObjects(collaborationIds)
        return DeleteCollaborationObjectResponse(deleteStatus)
    }
}
