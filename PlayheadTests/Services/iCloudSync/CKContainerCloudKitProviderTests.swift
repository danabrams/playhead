// CKContainerCloudKitProviderTests.swift
// playhead-5c1t — unit tests for the page-cap truncation logic in
// `CKContainerCloudKitProvider.collectPages`. The static helper is
// generic over Record and Cursor types, so the tests drive it with
// plain `Int` records and `Int` cursors instead of synthesizing a
// real `CKQueryOperation.Cursor` (which has no public initializer).

import Foundation
import OSLog
import Testing

@testable import Playhead

@Suite("CKContainerCloudKitProvider.collectPages")
struct CKContainerCloudKitProviderCollectPagesTests {
    private static let logger = Logger(
        subsystem: "com.playhead.tests",
        category: "iCloudSync.PageCap"
    )

    @Test("Single page with nil cursor returns its records and exits cleanly")
    func singlePageExitsImmediately() async throws {
        var firstCalls = 0
        var nextCalls = 0
        let result: [Int] = try await CKContainerCloudKitProvider.collectPages(
            pageSize: 200,
            maxPages: 200,
            recordTypeForLog: "T",
            logger: Self.logger,
            fetchFirst: { _ -> CKContainerCloudKitProvider.PageResult<Int, Int> in
                firstCalls += 1
                return .init(records: [1, 2], cursor: nil)
            },
            fetchNext: { _, _ -> CKContainerCloudKitProvider.PageResult<Int, Int> in
                nextCalls += 1
                Issue.record("fetchNext should not be invoked when first page returns nil cursor")
                return .init(records: [], cursor: nil)
            }
        )

        #expect(result == [1, 2])
        #expect(firstCalls == 1)
        #expect(nextCalls == 0)
    }

    @Test("Cursor pagination collects records from every page in order")
    func cursorPaginationFollowsCursor() async throws {
        // Three pages, then nil cursor. fetchNext is asked to follow the
        // cursor returned by the previous page; the tests assert
        // (a) the right number of fetchNext calls, and (b) cursors get
        // threaded through (each fetchNext receives the cursor produced
        // by its predecessor).
        let pageRecords: [[Int]] = [[1, 2], [3, 4], [5, 6]]
        let pageCursors: [Int?] = [10, 20, nil]
        var observedCursors: [Int] = []
        var nextCallIndex = 0

        let result: [Int] = try await CKContainerCloudKitProvider.collectPages(
            pageSize: 200,
            maxPages: 200,
            recordTypeForLog: "T",
            logger: Self.logger,
            fetchFirst: { _ -> CKContainerCloudKitProvider.PageResult<Int, Int> in
                .init(records: pageRecords[0], cursor: pageCursors[0])
            },
            fetchNext: { cursor, _ -> CKContainerCloudKitProvider.PageResult<Int, Int> in
                observedCursors.append(cursor)
                nextCallIndex += 1
                return .init(
                    records: pageRecords[nextCallIndex],
                    cursor: pageCursors[nextCallIndex]
                )
            }
        )

        #expect(result == [1, 2, 3, 4, 5, 6])
        #expect(nextCallIndex == 2)
        // First fetchNext gets cursor from page 0; second gets cursor
        // from page 1.
        #expect(observedCursors == [10, 20])
    }

    @Test("maxPages cap truncates when cursor never goes nil")
    func maxPagesCapTruncates() async throws {
        // Always-non-nil cursor: verifies the loop exits at the cap and
        // does NOT iterate forever. Counts the number of fetchNext
        // invocations to assert the cap is exact.
        let cap = 5
        var firstCalls = 0
        var nextCalls = 0
        let result: [Int] = try await CKContainerCloudKitProvider.collectPages(
            pageSize: 10,
            maxPages: cap,
            recordTypeForLog: "T",
            logger: Self.logger,
            fetchFirst: { limit -> CKContainerCloudKitProvider.PageResult<Int, Int> in
                firstCalls += 1
                return .init(records: Array(repeating: 0, count: limit), cursor: 1)
            },
            fetchNext: { _, limit -> CKContainerCloudKitProvider.PageResult<Int, Int> in
                nextCalls += 1
                return .init(records: Array(repeating: 0, count: limit), cursor: 1)
            }
        )

        #expect(firstCalls == 1)
        // First page counts as page 1; cap=5 → 4 follow-up fetches.
        #expect(nextCalls == cap - 1)
        #expect(result.count == cap * 10)
    }

    @Test("Cap exactly equals page count: no truncation log expected")
    func capEqualsPageCountExits() async throws {
        // Three pages, third returns nil cursor, cap=3. The loop should
        // consume exactly 3 pages without the cap firing prematurely.
        let cap = 3
        let pageCursors: [Int?] = [1, 2, nil]
        var nextCalls = 0

        let result: [Int] = try await CKContainerCloudKitProvider.collectPages(
            pageSize: 5,
            maxPages: cap,
            recordTypeForLog: "T",
            logger: Self.logger,
            fetchFirst: { _ -> CKContainerCloudKitProvider.PageResult<Int, Int> in
                .init(records: Array(repeating: 0, count: 5), cursor: pageCursors[0])
            },
            fetchNext: { _, _ -> CKContainerCloudKitProvider.PageResult<Int, Int> in
                nextCalls += 1
                return .init(records: Array(repeating: 0, count: 5), cursor: pageCursors[nextCalls])
            }
        )

        #expect(nextCalls == 2)
        #expect(result.count == 15)
    }

    @Test("Errors thrown from fetchFirst propagate")
    func fetchFirstErrorPropagates() async {
        struct Sentinel: Error {}
        await #expect(throws: Sentinel.self) {
            _ = try await CKContainerCloudKitProvider.collectPages(
                pageSize: 200,
                maxPages: 200,
                recordTypeForLog: "T",
                logger: Self.logger,
                fetchFirst: { _ -> CKContainerCloudKitProvider.PageResult<Int, Int> in
                    throw Sentinel()
                },
                fetchNext: { _, _ -> CKContainerCloudKitProvider.PageResult<Int, Int> in
                    .init(records: [], cursor: nil)
                }
            )
        }
    }

    @Test("Errors thrown from fetchNext propagate")
    func fetchNextErrorPropagates() async {
        struct Sentinel: Error {}
        await #expect(throws: Sentinel.self) {
            _ = try await CKContainerCloudKitProvider.collectPages(
                pageSize: 200,
                maxPages: 200,
                recordTypeForLog: "T",
                logger: Self.logger,
                fetchFirst: { _ -> CKContainerCloudKitProvider.PageResult<Int, Int> in
                    .init(records: [], cursor: 1)
                },
                fetchNext: { _, _ -> CKContainerCloudKitProvider.PageResult<Int, Int> in
                    throw Sentinel()
                }
            )
        }
    }
}
