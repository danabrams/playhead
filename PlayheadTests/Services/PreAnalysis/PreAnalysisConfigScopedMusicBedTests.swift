// PreAnalysisConfigScopedMusicBedTests.swift
// playhead-2hpn: lock the rollback-friendly defaults for the
// `scopedMusicBedGeneralization` feature flag. These tests verify:
//
//   * Default-constructed configs have the flag OFF.
//   * Configs persisted before the flag was added (i.e. JSON blobs
//     that lack the `scopedMusicBedGeneralization` key) decode with
//     the flag OFF.
//   * Encode / decode round-trip preserves the flag when it is ON.

import Foundation
import Testing

@testable import Playhead

@Suite("PreAnalysisConfig — scopedMusicBedGeneralization flag")
struct PreAnalysisConfigScopedMusicBedTests {

    @Test("default-constructed config has the flag OFF")
    func defaultFlagOff() {
        let cfg = PreAnalysisConfig()
        #expect(cfg.scopedMusicBedGeneralization == false,
                "Flag default MUST be OFF so production keeps byte-identical pre-2hpn behavior")
    }

    @Test("legacy JSON without the flag decodes as OFF")
    func legacyJsonDecodesOff() throws {
        // Synthetic pre-2hpn config blob — explicitly omits the new key.
        let json = """
        {
            "isEnabled": true,
            "defaultT0DepthSeconds": 90,
            "t1DepthSeconds": 300,
            "t2DepthSeconds": 900,
            "useDualBackgroundSessions": false,
            "nominalShardDurationSec": 20
        }
        """
        let data = Data(json.utf8)
        let cfg = try JSONDecoder().decode(PreAnalysisConfig.self, from: data)
        #expect(cfg.scopedMusicBedGeneralization == false,
                "Legacy JSON without the flag MUST decode as OFF (rollback safety)")
    }

    @Test("flag round-trips through encode / decode")
    func flagRoundTrips() throws {
        var cfg = PreAnalysisConfig()
        cfg.scopedMusicBedGeneralization = true
        let data = try JSONEncoder().encode(cfg)
        let decoded = try JSONDecoder().decode(PreAnalysisConfig.self, from: data)
        #expect(decoded.scopedMusicBedGeneralization == true)
    }
}
