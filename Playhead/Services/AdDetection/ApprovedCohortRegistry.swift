// ApprovedCohortRegistry.swift
// Pure rollout gate for Foundation Model backfill cohorts.

import Foundation

struct ApprovedCohortRegistry: Sendable, Codable, Equatable {
    private var decisions: [CohortKey: Decision]

    init(decisions: [CohortKey: Decision] = [:]) {
        self.decisions = decisions
    }

    func effectiveMode(
        osBuild: String,
        scanCohort: ScanCohort,
        requestedMode: FMBackfillMode? = nil
    ) -> FMBackfillMode {
        let key = CohortKey(osBuild: osBuild, scanCohort: scanCohort)
        guard let decision = decisions[key] else {
            guard let requestedMode else {
                return .shadow
            }
            return Self.intersection(approvedMode: .shadow, requestedMode: requestedMode)
        }

        switch decision.status {
        case .approved:
            let approvedMode = decision.approvedMode ?? .shadow
            guard let requestedMode else {
                return approvedMode
            }
            return Self.intersection(approvedMode: approvedMode, requestedMode: requestedMode)
        case .knownBad:
            return .off
        }
    }

    private static func intersection(
        approvedMode: FMBackfillMode,
        requestedMode: FMBackfillMode
    ) -> FMBackfillMode {
        if approvedMode == .off || requestedMode == .off {
            return .off
        }
        if approvedMode == .shadow || requestedMode == .shadow {
            return .shadow
        }

        let canRescore = approvedMode.contributesToExistingCandidateLedger &&
            requestedMode.contributesToExistingCandidateLedger
        let canPropose = approvedMode.canProposeNewRegions &&
            requestedMode.canProposeNewRegions

        switch (canRescore, canPropose) {
        case (true, true):
            return .full
        case (true, false):
            return .rescoreOnly
        case (false, true):
            return .proposalOnly
        case (false, false):
            return .shadow
        }
    }

    func decision(
        osBuild: String,
        scanCohort: ScanCohort
    ) -> Decision? {
        decisions[CohortKey(osBuild: osBuild, scanCohort: scanCohort)]
    }

    mutating func approve(
        osBuild: String,
        scanCohort: ScanCohort,
        mode: FMBackfillMode
    ) {
        let key = CohortKey(osBuild: osBuild, scanCohort: scanCohort)
        decisions[key] = Decision(
            status: .approved,
            approvedMode: mode,
            reason: nil
        )
    }

    mutating func markKnownBad(
        osBuild: String,
        scanCohort: ScanCohort,
        reason: String? = nil
    ) {
        let key = CohortKey(osBuild: osBuild, scanCohort: scanCohort)
        decisions[key] = Decision(
            status: .knownBad,
            approvedMode: decisions[key]?.approvedMode,
            reason: reason
        )
    }

    mutating func remove(
        osBuild: String,
        scanCohort: ScanCohort
    ) {
        decisions.removeValue(forKey: CohortKey(osBuild: osBuild, scanCohort: scanCohort))
    }
}

extension ApprovedCohortRegistry {
    struct CohortKey: Sendable, Codable, Hashable {
        let osBuild: String
        let scanCohortIdentity: String

        init(osBuild: String, scanCohort: ScanCohort) {
            self.osBuild = osBuild
            self.scanCohortIdentity = Self.canonicalIdentity(for: scanCohort)
        }

        static func canonicalIdentity(for scanCohort: ScanCohort) -> String {
            let encoder = JSONEncoder()
            encoder.outputFormatting = [.sortedKeys]
            let identity = ScanCohortIdentity(scanCohort: scanCohort)
            // ScanCohortIdentity is a value-only Codable type, so encoding
            // cannot fail unless the type itself changes to include throwing fields.
            let data = try? encoder.encode(identity)
            return data.flatMap { String(data: $0, encoding: .utf8) } ?? "{}"
        }

        private struct ScanCohortIdentity: Codable {
            let promptLabel: String
            let promptHash: String
            let schemaHash: String
            let scanPlanHash: String
            let normalizationHash: String
            let locale: String
            let appBuild: String

            init(scanCohort: ScanCohort) {
                promptLabel = scanCohort.promptLabel
                promptHash = scanCohort.promptHash
                schemaHash = scanCohort.schemaHash
                scanPlanHash = scanCohort.scanPlanHash
                normalizationHash = scanCohort.normalizationHash
                locale = scanCohort.locale
                appBuild = scanCohort.appBuild
            }
        }
    }

    struct Decision: Sendable, Codable, Equatable {
        let status: Status
        let approvedMode: FMBackfillMode?
        let reason: String?
    }

    enum Status: String, Sendable, Codable, Equatable {
        case approved
        case knownBad
    }
}
