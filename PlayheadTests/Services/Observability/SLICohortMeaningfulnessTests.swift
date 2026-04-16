// SLICohortMeaningfulnessTests.swift
// Tests for the (SLI × cohort) → isMeaningful mapping.
//
// Every acceptance rule the bead specifies is covered here:
// - time_to_proximal_skip_ready is only meaningful in eligibleAndAvailable mode.
// - The two latency SLIs require a 30–90 min episode and explicit download.
// - false_ready_rate requires eligibleAndAvailable mode (can't be falsely
//   ready if analysis never ran).
// - ready_by_first_play_rate and unattributed_pause_rate are universal.
//
// These tests also exercise the convenience method `SLI.isMeaningful(for:)`
// which should produce identical results to the underlying
// SLICohortMeaningfulness type.

import Foundation
import Testing

@testable import Playhead

@Suite("SLI × Cohort Meaningfulness")
struct SLICohortMeaningfulnessTests {

    // MARK: - Helpers

    private func cohort(
        trigger: SLITrigger = .explicitDownload,
        mode: SLIAnalysisMode = .eligibleAndAvailable,
        condition: SLIExecutionCondition = .favorable,
        bucket: SLIEpisodeDurationBucket = .between30and60m
    ) -> SLICohort {
        SLICohort(
            trigger: trigger,
            analysisMode: mode,
            executionCondition: condition,
            durationBucket: bucket
        )
    }

    // MARK: - time_to_downloaded

    @Test("time_to_downloaded: meaningful for explicit + 30-60")
    func t2dExplicit30To60() {
        let c = cohort(trigger: .explicitDownload, bucket: .between30and60m)
        #expect(SLI.timeToDownloaded.isMeaningful(for: c))
    }

    @Test("time_to_downloaded: meaningful for explicit + 60-90")
    func t2dExplicit60To90() {
        let c = cohort(trigger: .explicitDownload, bucket: .between60and90m)
        #expect(SLI.timeToDownloaded.isMeaningful(for: c))
    }

    @Test("time_to_downloaded: NOT meaningful for under30m")
    func t2dUnder30m() {
        let c = cohort(trigger: .explicitDownload, bucket: .under30m)
        #expect(!SLI.timeToDownloaded.isMeaningful(for: c))
    }

    @Test("time_to_downloaded: NOT meaningful for over90m")
    func t2dOver90m() {
        let c = cohort(trigger: .explicitDownload, bucket: .over90m)
        #expect(!SLI.timeToDownloaded.isMeaningful(for: c))
    }

    @Test("time_to_downloaded: NOT meaningful for subscriptionAutoDownload")
    func t2dSubscription() {
        let c = cohort(trigger: .subscriptionAutoDownload, bucket: .between30and60m)
        #expect(!SLI.timeToDownloaded.isMeaningful(for: c))
    }

    // MARK: - time_to_proximal_skip_ready

    @Test("time_to_proximal_skip_ready: meaningful for explicit + eligibleAndAvailable + 30-90")
    func t2pEligibleAndAvailable() {
        let c = cohort(
            trigger: .explicitDownload,
            mode: .eligibleAndAvailable,
            bucket: .between60and90m
        )
        #expect(SLI.timeToProximalSkipReady.isMeaningful(for: c))
    }

    @Test("time_to_proximal_skip_ready: NOT meaningful for transportOnly mode")
    func t2pTransportOnly() {
        let c = cohort(
            trigger: .explicitDownload,
            mode: .transportOnly,
            bucket: .between30and60m
        )
        #expect(!SLI.timeToProximalSkipReady.isMeaningful(for: c))
    }

    @Test("time_to_proximal_skip_ready: NOT meaningful for eligibleButUnavailableNow mode")
    func t2pEligibleButUnavailable() {
        let c = cohort(
            trigger: .explicitDownload,
            mode: .eligibleButUnavailableNow,
            bucket: .between30and60m
        )
        #expect(!SLI.timeToProximalSkipReady.isMeaningful(for: c))
    }

    @Test("time_to_proximal_skip_ready: NOT meaningful for subscription auto-download")
    func t2pSubscriptionAuto() {
        let c = cohort(
            trigger: .subscriptionAutoDownload,
            mode: .eligibleAndAvailable,
            bucket: .between30and60m
        )
        #expect(!SLI.timeToProximalSkipReady.isMeaningful(for: c))
    }

    @Test("time_to_proximal_skip_ready: NOT meaningful for out-of-scope duration")
    func t2pOutOfScopeDuration() {
        let c = cohort(
            trigger: .explicitDownload,
            mode: .eligibleAndAvailable,
            bucket: .under30m
        )
        #expect(!SLI.timeToProximalSkipReady.isMeaningful(for: c))
    }

    // MARK: - ready_by_first_play_rate

    @Test("ready_by_first_play_rate: universal — meaningful across every cohort")
    func readyByFirstPlayUniversal() {
        for trigger in SLITrigger.allCases {
            for mode in SLIAnalysisMode.allCases {
                for condition in SLIExecutionCondition.allCases {
                    for bucket in SLIEpisodeDurationBucket.allCases {
                        let c = SLICohort(
                            trigger: trigger,
                            analysisMode: mode,
                            executionCondition: condition,
                            durationBucket: bucket
                        )
                        #expect(
                            SLI.readyByFirstPlayRate.isMeaningful(for: c),
                            "ready_by_first_play_rate must be meaningful for cohort \(c)"
                        )
                    }
                }
            }
        }
    }

    // MARK: - false_ready_rate

    @Test("false_ready_rate: meaningful only in eligibleAndAvailable mode")
    func falseReadyEligibleAndAvailable() {
        let eligible = cohort(mode: .eligibleAndAvailable)
        #expect(SLI.falseReadyRate.isMeaningful(for: eligible))

        let transport = cohort(mode: .transportOnly)
        #expect(!SLI.falseReadyRate.isMeaningful(for: transport))

        let deferred = cohort(mode: .eligibleButUnavailableNow)
        #expect(!SLI.falseReadyRate.isMeaningful(for: deferred))
    }

    // MARK: - unattributed_pause_rate

    @Test("unattributed_pause_rate: universal — meaningful across every cohort")
    func unattributedPauseUniversal() {
        for trigger in SLITrigger.allCases {
            for mode in SLIAnalysisMode.allCases {
                for condition in SLIExecutionCondition.allCases {
                    for bucket in SLIEpisodeDurationBucket.allCases {
                        let c = SLICohort(
                            trigger: trigger,
                            analysisMode: mode,
                            executionCondition: condition,
                            durationBucket: bucket
                        )
                        #expect(
                            SLI.unattributedPauseRate.isMeaningful(for: c),
                            "unattributed_pause_rate must be meaningful for cohort \(c)"
                        )
                    }
                }
            }
        }
    }

    // MARK: - SLI.isMeaningful delegates to SLICohortMeaningfulness

    @Test("SLI.isMeaningful matches SLICohortMeaningfulness for every SLI × cohort cell")
    func delegationMatchesForAllCells() {
        for sli in SLI.allCases {
            for trigger in SLITrigger.allCases {
                for mode in SLIAnalysisMode.allCases {
                    for condition in SLIExecutionCondition.allCases {
                        for bucket in SLIEpisodeDurationBucket.allCases {
                            let c = SLICohort(
                                trigger: trigger,
                                analysisMode: mode,
                                executionCondition: condition,
                                durationBucket: bucket
                            )
                            let direct = SLICohortMeaningfulness.isMeaningful(
                                sli: sli,
                                cohort: c
                            )
                            let delegated = sli.isMeaningful(for: c)
                            #expect(
                                direct == delegated,
                                "Mismatch for \(sli) on \(c): direct=\(direct), delegated=\(delegated)"
                            )
                        }
                    }
                }
            }
        }
    }
}
