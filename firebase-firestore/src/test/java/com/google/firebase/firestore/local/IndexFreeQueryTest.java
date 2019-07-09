// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.firebase.firestore.local;

import static com.google.firebase.firestore.testutil.TestUtil.doc;
import static com.google.firebase.firestore.testutil.TestUtil.filter;
import static com.google.firebase.firestore.testutil.TestUtil.map;
import static com.google.firebase.firestore.testutil.TestUtil.query;
import static com.google.firebase.firestore.testutil.TestUtil.values;
import static com.google.firebase.firestore.testutil.TestUtil.version;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;

import com.google.firebase.database.collection.ImmutableSortedMap;
import com.google.firebase.database.collection.ImmutableSortedSet;
import com.google.firebase.firestore.core.Query;
import com.google.firebase.firestore.model.Document;
import com.google.firebase.firestore.model.DocumentCollections;
import com.google.firebase.firestore.model.DocumentKey;
import com.google.firebase.firestore.model.MaybeDocument;
import com.google.firebase.firestore.model.SnapshotVersion;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.robolectric.RobolectricTestRunner;
import org.robolectric.annotation.Config;

@RunWith(RobolectricTestRunner.class)
@Config(manifest = Config.NONE)
public class IndexFreeQueryTest {

  private static final Document MATCHING_DOC_A =
      doc("foo/a", 1, map("matches", true), Document.DocumentState.SYNCED);
  private static final Document NON_MATCHING_DOC_A =
      doc("foo/a", 1, map("matches", false), Document.DocumentState.SYNCED);
  private static final Document MATCHING_DOC_B =
      doc("foo/b", 1, map("matches", true), Document.DocumentState.SYNCED);
  private static final Document NON_MATCHING_DOC_B =
      doc("foo/b", 1, map("matches", false), Document.DocumentState.SYNCED);
  private static final Document UPDATED_MATCHING_DOC_B =
      doc("foo/b", 11, map("matches", true), Document.DocumentState.SYNCED);

  private static final int TARGET_ID = 1;

  private QueryEngine queryEngine;

  @Mock private LocalDocumentsView localDocumentsView;
  @Mock private QueryCache queryCache;

  private Map<Integer, ImmutableSortedSet<DocumentKey>> keysByTarget = new HashMap<>();
  private Map<DocumentKey, Document> documentsByKey = new HashMap<>();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    queryEngine = new IndexFreeQueryEngine(localDocumentsView, queryCache);

    doAnswer(
            getMatchingKeysForTargetIdInvocation -> {
              int targetId = getMatchingKeysForTargetIdInvocation.getArgument(0);
              return keysByTarget.get(targetId);
            })
        .when(queryCache)
        .getMatchingKeysForTargetId(anyInt());

    doAnswer(
            getDocumentsInvocation -> {
              Iterable<DocumentKey> keys = getDocumentsInvocation.getArgument(0);

              ImmutableSortedMap<DocumentKey, MaybeDocument> docs =
                  DocumentCollections.emptyMaybeDocumentMap();
              for (DocumentKey key : keys) {
                docs = docs.insert(key, documentsByKey.get(key));
              }

              return docs;
            })
        .when(localDocumentsView)
        .getDocuments(any());

    doAnswer(
            getDocumentsMatchingQueryInvocation -> {
              Query query = getDocumentsMatchingQueryInvocation.getArgument(0);
              SnapshotVersion snapshotVersion = getDocumentsMatchingQueryInvocation.getArgument(1);

              ImmutableSortedMap<DocumentKey, MaybeDocument> matchingDocs =
                  DocumentCollections.emptyMaybeDocumentMap();

              for (Document doc : documentsByKey.values()) {
                if (query.matches(doc) && snapshotVersion.compareTo(doc.getVersion()) >= 0) {
                  matchingDocs = matchingDocs.insert(doc.getKey(), doc);
                }
              }

              return matchingDocs;
            })
        .when(localDocumentsView)
        .getDocumentsMatchingQuery(any(), any());
  }

  private void addResult(int targetId, Document doc) {
    ImmutableSortedSet<DocumentKey> currentKeys = keysByTarget.get(targetId);

    if (currentKeys == null) {
      currentKeys = DocumentKey.emptyKeySet().insert(doc.getKey());
    } else {
      currentKeys = currentKeys.insert(doc.getKey());
    }

    keysByTarget.put(targetId, currentKeys);
    documentsByKey.put(doc.getKey(), doc);
  }

  @Test
  public void usesTargetMappingForInitialResults() {
    Query query = query("foo").filter(filter("matches", "==", true));
    QueryData queryData = queryData(query, true);

    addResult(TARGET_ID, MATCHING_DOC_A);
    addResult(TARGET_ID, MATCHING_DOC_B);

    ImmutableSortedMap<DocumentKey, Document> docs =
        queryEngine.getDocumentsMatchingQuery(query, queryData);
    assertEquals(asList(MATCHING_DOC_A, MATCHING_DOC_B), values(docs));
  }

  @Test
  public void filterNonMatchingInitialResults() {
    Query query = query("foo").filter(filter("matches", "==", true));
    QueryData queryData = queryData(query, true);

    addResult(TARGET_ID, NON_MATCHING_DOC_A);
    addResult(TARGET_ID, MATCHING_DOC_B);

    ImmutableSortedMap<DocumentKey, Document> docs =
        queryEngine.getDocumentsMatchingQuery(query, queryData);
    assertEquals(asList(MATCHING_DOC_B), values(docs));
  }

  @Test
  public void includesChangesSinceInitialResults() {
    Query query = query("foo").filter(filter("matches", "==", true));
    QueryData queryData = queryData(query, true);

    addResult(TARGET_ID, MATCHING_DOC_A);
    addResult(TARGET_ID, NON_MATCHING_DOC_B);

    ImmutableSortedMap<DocumentKey, Document> docs =
        queryEngine.getDocumentsMatchingQuery(query, queryData);
    assertEquals(asList(MATCHING_DOC_A), values(docs));

    addResult(TARGET_ID, UPDATED_MATCHING_DOC_B);

    docs = queryEngine.getDocumentsMatchingQuery(query, queryData);
    assertEquals(asList(MATCHING_DOC_A, UPDATED_MATCHING_DOC_B), values(docs));
  }

  @Test
  public void doesNotUseInitialResultsForNonSyncedQuery() {
    doThrow(AssertionError.class).when(queryCache).getMatchingKeysForTargetId(anyInt());

    Query query = query("foo").filter(filter("matches", "==", true));
    QueryData queryData = queryData(query, false);

    ImmutableSortedMap<DocumentKey, Document> docs =
        queryEngine.getDocumentsMatchingQuery(query, queryData);
    assertEquals(asList(), values(docs));
  }

  @Test
  public void doesNotUseInitialResultsForUnfilteredCollectionQuery() {
    doThrow(AssertionError.class).when(queryCache).getMatchingKeysForTargetId(anyInt());

    Query query = query("foo");
    QueryData queryData = queryData(query, true);

    ImmutableSortedMap<DocumentKey, Document> docs =
        queryEngine.getDocumentsMatchingQuery(query, queryData);
    assertEquals(asList(), values(docs));
  }

  private QueryData queryData(Query query, boolean synced) {
    return new QueryData(
        query, TARGET_ID, 1, synced, QueryPurpose.LISTEN, version(10), ByteString.EMPTY);
  }
}
