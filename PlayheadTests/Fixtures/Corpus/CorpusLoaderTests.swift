// CorpusLoaderTests.swift
// Tests for the labeled test corpus loader and annotation validation.

import Foundation
import Testing
@testable import Playhead

// MARK: - Corpus Loader Tests

@Suite("CorpusLoader – Loading & Validation")
struct CorpusLoaderBasicTests {

    let loader = CorpusLoader()

    @Test("Loads the corpus manifest")
    func loadManifest() throws {
        let manifest = try loader.loadManifest()

        #expect(manifest.corpusVersion == "1.0.0")
        #expect(manifest.episodes.count == 15, "Corpus should have 15 episodes")
    }

    @Test("Loads all annotations without error")
    func loadAllAnnotations() throws {
        let annotations = try loader.loadAllAnnotations()

        #expect(annotations.count == 15)
        for ann in annotations {
            #expect(ann.schemaVersion == TestEpisodeAnnotation.currentSchemaVersion)
        }
    }

    @Test("All annotations pass validation")
    func validateAll() throws {
        let annotations = try loader.loadAllAnnotations()

        for ann in annotations {
            let errors = loader.validate(ann)
            #expect(errors.isEmpty, "Validation errors for \(ann.annotationId): \(errors)")
        }
    }

    @Test("Loads annotations by tag")
    func loadByTag() throws {
        let hostRead = try loader.loadAnnotations(withTag: "host-read")
        #expect(!hostRead.isEmpty, "Should find episodes tagged host-read")
        for ann in hostRead {
            #expect(ann.tags.contains("host-read"))
        }
    }

    @Test("Loads annotations for per-show priors testing")
    func perShowPriors() throws {
        let techWeekly = try loader.loadAnnotations(forPodcastId: "pod-tech-weekly")
        #expect(techWeekly.count == 2, "Should have 2 Tech Weekly episodes for priors testing")
        #expect(techWeekly.allSatisfy { $0.podcast.podcastId == "pod-tech-weekly" })
    }

    @Test("No-ad episode has empty segments")
    func noAdEpisode() throws {
        let annotations = try loader.loadAllAnnotations()
        let noAd = annotations.first { $0.isNoAdEpisode }

        #expect(noAd != nil, "Should have a no-ad episode")
        #expect(noAd?.adSegments.isEmpty == true)
    }
}

@Suite("CorpusLoader – Annotation Content")
struct CorpusAnnotationContentTests {

    let loader = CorpusLoader()

    @Test("Tech Weekly has dynamic insertion ads")
    func dynamicInsertionAds() throws {
        let annotations = try loader.loadAnnotations(forPodcastId: "pod-tech-weekly")
        let ep142 = annotations.first { $0.episode.episodeId == "ep-tech-142" }

        #expect(ep142 != nil)
        #expect(ep142!.podcast.usesDynamicAdInsertion)
        #expect(ep142!.adSegments.allSatisfy { $0.deliveryStyle == .dynamicInsertion })
    }

    @Test("Comedy episode has back-to-back ads")
    func backToBackAds() throws {
        let annotations = try loader.loadAllAnnotations()
        let comedy = annotations.first { $0.episode.episodeId == "ep-comedy-301" }

        #expect(comedy != nil)
        let segments = comedy!.adSegments.sorted { $0.startTime < $1.startTime }

        // Three contiguous segments: end of one == start of next.
        #expect(segments.count >= 3)
        #expect(segments[0].endTime == segments[1].startTime, "First two ads should be contiguous")
        #expect(segments[1].endTime == segments[2].startTime, "Second two ads should be contiguous")
    }

    @Test("Blended host-read episodes are tagged as hard difficulty")
    func blendedHostReadDifficulty() throws {
        let annotations = try loader.loadAnnotations(withTag: "blended-host-read")
        #expect(!annotations.isEmpty)

        for ann in annotations {
            let blended = ann.adSegments.filter { $0.deliveryStyle == .blendedHostRead }
            #expect(!blended.isEmpty, "\(ann.annotationId) should have blended host-read segments")
            for seg in blended {
                #expect(seg.difficulty == .hard, "Blended host-read should be hard difficulty")
            }
        }
    }

    @Test("Very short ad exists for edge-case testing")
    func veryShortAd() throws {
        let annotations = try loader.loadAnnotations(withTag: "very-short-ad")
        #expect(!annotations.isEmpty)

        let shortAd = annotations.flatMap(\.adSegments).first { $0.duration < 15 }
        #expect(shortAd != nil, "Should have an ad shorter than 15 seconds")
        #expect(shortAd!.duration < 15, "Short ad should be under 15s (was \(shortAd!.duration)s)")
    }

    @Test("All episodes have positive durations and valid time ranges")
    func timeRangeValidity() throws {
        let annotations = try loader.loadAllAnnotations()

        for ann in annotations {
            #expect(ann.episode.duration > 0, "\(ann.annotationId): duration must be positive")
            for (i, seg) in ann.adSegments.enumerated() {
                #expect(seg.startTime >= 0, "\(ann.annotationId) seg \(i): startTime negative")
                #expect(seg.endTime > seg.startTime, "\(ann.annotationId) seg \(i): endTime <= startTime")
                #expect(seg.endTime <= ann.episode.duration, "\(ann.annotationId) seg \(i): endTime > duration")
            }
        }
    }

    @Test("Corpus covers required genre diversity")
    func genreDiversity() throws {
        let annotations = try loader.loadAllAnnotations()
        let genres = Set(annotations.map(\.podcast.genre))

        let requiredGenres = ["Technology", "True Crime", "Comedy", "News", "History", "Science"]
        for genre in requiredGenres {
            #expect(genres.contains(genre), "Corpus should include \(genre) genre")
        }
    }
}

@Suite("CorpusLoader – Validation Edge Cases")
struct CorpusValidationEdgeCaseTests {

    let loader = CorpusLoader()

    @Test("Validates overlapping segments")
    func overlappingSegments() {
        let annotation = TestEpisodeAnnotation(
            annotationId: "test-overlap",
            audioFileReference: "test.m4a",
            podcast: TestPodcastMetadata(
                podcastId: "test", title: "Test", author: "Test",
                genre: "Test", usesDynamicAdInsertion: false
            ),
            episode: TestEpisodeMetadata(
                episodeId: "test-ep", title: "Test Episode",
                duration: 600, publishedAt: "2026-01-01",
                feedURL: "https://example.com/feed",
                audioURL: "https://example.com/audio.m4a"
            ),
            adSegments: [
                TestAdSegment(startTime: 100, endTime: 200, advertiser: nil, product: nil,
                              adType: .midRoll, deliveryStyle: .hostRead, difficulty: .easy, notes: nil),
                TestAdSegment(startTime: 150, endTime: 250, advertiser: nil, product: nil,
                              adType: .midRoll, deliveryStyle: .hostRead, difficulty: .easy, notes: nil),
            ],
            isNoAdEpisode: false,
            tags: [],
            schemaVersion: 1,
            annotatedBy: "test",
            lastUpdated: "2026-01-01"
        )

        let errors = loader.validate(annotation)
        #expect(errors.contains { $0.contains("overlap") }, "Should detect overlapping segments")
    }

    @Test("Validates no-ad flag consistency")
    func noAdFlagInconsistency() {
        let annotation = TestEpisodeAnnotation(
            annotationId: "test-noAd",
            audioFileReference: "test.m4a",
            podcast: TestPodcastMetadata(
                podcastId: "test", title: "Test", author: "Test",
                genre: "Test", usesDynamicAdInsertion: false
            ),
            episode: TestEpisodeMetadata(
                episodeId: "test-ep", title: "Test Episode",
                duration: 600, publishedAt: "2026-01-01",
                feedURL: "https://example.com/feed",
                audioURL: "https://example.com/audio.m4a"
            ),
            adSegments: [
                TestAdSegment(startTime: 100, endTime: 200, advertiser: nil, product: nil,
                              adType: .midRoll, deliveryStyle: .hostRead, difficulty: .easy, notes: nil),
            ],
            isNoAdEpisode: true,
            tags: [],
            schemaVersion: 1,
            annotatedBy: "test",
            lastUpdated: "2026-01-01"
        )

        let errors = loader.validate(annotation)
        #expect(errors.contains { $0.contains("isNoAdEpisode") }, "Should detect no-ad flag inconsistency")
    }

    @Test("Validation accepts empty segment lists")
    func emptySegmentsDoNotTrap() {
        let annotation = TestEpisodeAnnotation(
            annotationId: "test-empty",
            audioFileReference: "test.m4a",
            podcast: TestPodcastMetadata(
                podcastId: "test", title: "Test", author: "Test",
                genre: "Test", usesDynamicAdInsertion: false
            ),
            episode: TestEpisodeMetadata(
                episodeId: "test-ep", title: "Test Episode",
                duration: 600, publishedAt: "2026-01-01",
                feedURL: "https://example.com/feed",
                audioURL: "https://example.com/audio.m4a"
            ),
            adSegments: [],
            isNoAdEpisode: true,
            tags: [],
            schemaVersion: 1,
            annotatedBy: "test",
            lastUpdated: "2026-01-01"
        )

        let errors = loader.validate(annotation)
        #expect(errors.isEmpty, "Empty no-ad annotations should validate cleanly")
    }

    @Test("Loads the ym57 fixture manifest and locked-core 8")
    func loadFixtureManifest() throws {
        let manifest = try loader.loadFixtureManifest()
        #expect(manifest.version == FixtureManifest.currentVersion)
        #expect(manifest.fixtures.count >= 8)

        let locked = try loader.loadLockedCoreFixtures()
        #expect(locked.count == 8)
        #expect(locked.map(\.slot) == Array(1...8), "Locked-core must be slot-ordered 1..8")
        let allLocked = locked.allSatisfy(\.locked)
        #expect(allLocked, "Every locked-core fixture should report locked == true")

        // Every locked-core fixture must resolve to a real file on disk.
        for fx in locked {
            let url = try loader.mediaURL(for: fx)
            #expect(FileManager.default.fileExists(atPath: url.path),
                    "Missing media file for \(fx.id) at \(url.path)")
        }
    }

    @Test("Fixture lookup by id returns the right descriptor")
    func fixtureLookupById() throws {
        let fx = try loader.fixture(withId: "fixture-01-30min-clean-speech")
        #expect(fx.slot == 1)
        #expect(fx.taxonomy.durationBucket == .m30)
        #expect(fx.taxonomy.language == "en-US")
    }

    @Test("Converts annotation to ReplayConfiguration")
    func annotationToReplayConfig() throws {
        let annotations = try loader.loadAllAnnotations()
        let first = annotations[0]

        let condition = SimulationCondition(
            audioMode: .cached,
            playbackSpeed: 1.0,
            interactions: [],
            analysisPath: .backfill
        )
        let config = loader.makeReplayConfig(from: first, condition: condition)

        #expect(config.episodeId == first.episode.episodeId)
        #expect(config.podcastId == first.podcast.podcastId)
        #expect(config.episodeDuration == first.episode.duration)
        #expect(config.groundTruthSegments.count == first.adSegments.count)
        #expect(
            config.groundTruthSegments.first?.deliveryStyle.rawValue
                == first.adSegments.first?.deliveryStyle.rawValue
        )
        #expect(!config.transcriptChunks.isEmpty, "Should generate synthetic transcript chunks")
        #expect(config.condition.analysisPath == .backfill)
    }
}
