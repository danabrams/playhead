// PreAnalysisConfigCreatorChapterFusionTests.swift
// playhead-rxuv: lock the rollback-friendly defaults for the
// `creatorChapterFusionEnabled` feature flag. Verifies:
//
//   * Default-constructed configs have the flag OFF.
//   * Configs persisted before the flag was added (i.e. JSON blobs
//     that lack the `creatorChapterFusionEnabled` key) decode with
//     the flag OFF.
//   * Encode / decode round-trip preserves the flag when it is ON.

import Foundation
import Testing

@testable import Playhead

@Suite("PreAnalysisConfig — creatorChapterFusionEnabled flag")
struct PreAnalysisConfigCreatorChapterFusionTests {

    @Test("default-constructed config has the flag OFF")
    func defaultFlagOff() {
        let cfg = PreAnalysisConfig()
        #expect(cfg.creatorChapterFusionEnabled == false,
                "Flag default MUST be OFF so production keeps byte-identical pre-rxuv behavior")
    }

    @Test("legacy JSON without the flag decodes as OFF")
    func legacyJsonDecodesOff() throws {
        // Synthetic pre-rxuv config blob — explicitly omits the new key.
        let json = """
        {
            "isEnabled": true,
            "defaultT0DepthSeconds": 90,
            "t1DepthSeconds": 300,
            "t2DepthSeconds": 900,
            "useDualBackgroundSessions": false,
            "nominalShardDurationSec": 20,
            "scopedMusicBedGeneralization": false,
            "showCapabilityProfilesEnabled": false
        }
        """
        let data = Data(json.utf8)
        let cfg = try JSONDecoder().decode(PreAnalysisConfig.self, from: data)
        #expect(cfg.creatorChapterFusionEnabled == false,
                "Legacy JSON without the flag MUST decode as OFF (rollback safety)")
    }

    @Test("flag round-trips through encode / decode")
    func flagRoundTrips() throws {
        var cfg = PreAnalysisConfig()
        cfg.creatorChapterFusionEnabled = true
        let data = try JSONEncoder().encode(cfg)
        let decoded = try JSONDecoder().decode(PreAnalysisConfig.self, from: data)
        #expect(decoded.creatorChapterFusionEnabled == true)
    }

    @Test("init param sets the flag without disturbing other flags")
    func initParamIndependent() {
        let cfg = PreAnalysisConfig(creatorChapterFusionEnabled: true)
        #expect(cfg.creatorChapterFusionEnabled == true)
        // Other per-bead flags retain their documented defaults.
        #expect(cfg.scopedMusicBedGeneralization == false)
        #expect(cfg.showCapabilityProfilesEnabled == false)
        #expect(cfg.useAdaptiveDeviceProfile == true)
        #expect(cfg.b4RevalidationFromFeaturesEnabled == true)
    }
}
