/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.observability.collaboration.model

import org.apache.lucene.search.TotalHits
import org.opensearch.action.search.SearchResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.observability.model.RestTag.OBJECT_LIST_FIELD
import org.opensearch.observability.model.SearchResults

/**
* CollaborationObject search results
*/
internal class CollaborationObjectSearchResult : SearchResults<CollaborationObjectDoc> {
    /**
     * single item result constructor
     */
    constructor(objectItem: CollaborationObjectDoc) : super(OBJECT_LIST_FIELD, objectItem)
    /**
     * multiple items result constructor
     */
    constructor(objectList: List<CollaborationObjectDoc>) : this(
        0,
        objectList.size.toLong(),
        TotalHits.Relation.EQUAL_TO,
        objectList
    )

    /**
     * all param constructor
     */
    constructor(
        startIndex: Long,
        totalHits: Long,
        totalHitRelation: TotalHits.Relation,
        objectList: List<CollaborationObjectDoc>
    ) : super(startIndex, totalHits, totalHitRelation, OBJECT_LIST_FIELD, objectList)

    /**
     * Constructor used in transport action communication.
     * @param input StreamInput stream to deserialize data from.
     */
    constructor(input: StreamInput) : super(input, CollaborationObjectDoc.reader)

    /**
     * Construct object from XContentParser
     */
    constructor(parser: XContentParser) : super(parser, OBJECT_LIST_FIELD)

    /**
     * Construct object from SearchResponse
     */
    constructor(from: Long, response: SearchResponse, searchHitParser: SearchHitParser<CollaborationObjectDoc>) : super(
        from,
        response,
        searchHitParser,
        OBJECT_LIST_FIELD
    )

    /**
     * {@inheritDoc}
     */
    override fun parseItem(parser: XContentParser, useId: String?): CollaborationObjectDoc {
        return CollaborationObjectDoc.parse(parser, useId)
    }
}
