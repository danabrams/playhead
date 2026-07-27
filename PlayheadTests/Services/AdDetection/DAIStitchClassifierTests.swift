import Foundation
import Testing

@testable import Playhead

// playhead-xsdz.71 (Signal 1): table-driven, deterministic tests for the pure
// redirect-chain DAI-stitch classifier.
@Suite("DAIStitchClassifier")
struct DAIStitchClassifierTests {

    // MARK: - The THEMOVE chain (the motivating example)

    @Test("THEMOVE chain classifies as megaphone with DAI expected (stitcher wins over routing/hosting)")
    func themoveChain() {
        // pscrb.fm → podtrac → megaphone/mgln.ai → claritas/clrtpod → traffic.libsyn
        let result = DAIStitchClassifier.classify(redirectChainHosts: [
            "pscrb.fm",
            "dts.podtrac.com",
            "mgln.ai",
            "clrtpod.com",
            "traffic.libsyn.com",
        ])
        #expect(result.stitchNetwork == .megaphone)
        #expect(result.daiExpected == true)
        #expect(result.matchedHost == "mgln.ai")
    }

    // MARK: - Individual known stitch networks

    @Test("megaphone.fm host")
    func megaphoneHost() {
        let result = DAIStitchClassifier.classify(redirectChainHosts: ["dcs.megaphone.fm"])
        #expect(result.stitchNetwork == .megaphone)
        #expect(result.daiExpected == true)
        #expect(result.matchedHost == "dcs.megaphone.fm")
    }

    @Test("adswizz host")
    func adswizzHost() {
        let result = DAIStitchClassifier.classify(redirectChainHosts: ["stitcher.adswizz.com"])
        #expect(result.stitchNetwork == .adswizz)
        #expect(result.daiExpected == true)
    }

    @Test("art19 host")
    func art19Host() {
        let result = DAIStitchClassifier.classify(redirectChainHosts: ["rss.art19.com"])
        #expect(result.stitchNetwork == .art19)
        #expect(result.daiExpected == true)
    }

    @Test("omny host (omny.fm)")
    func omnyHost() {
        let result = DAIStitchClassifier.classify(redirectChainHosts: ["traffic.omny.fm"])
        #expect(result.stitchNetwork == .omny)
        #expect(result.daiExpected == true)
    }

    @Test("omny host (omnycontent.com)")
    func omnyContentHost() {
        let result = DAIStitchClassifier.classify(redirectChainHosts: ["www.omnycontent.com"])
        #expect(result.stitchNetwork == .omny)
        #expect(result.daiExpected == true)
    }

    @Test("simplecast host")
    func simplecastHost() {
        let result = DAIStitchClassifier.classify(redirectChainHosts: ["cdn.simplecast.com"])
        #expect(result.stitchNetwork == .simplecast)
        #expect(result.daiExpected == true)
    }

    @Test("podtrac-only chain (routing redirect) still expects DAI")
    func podtracOnly() {
        let result = DAIStitchClassifier.classify(redirectChainHosts: [
            "dts.podtrac.com", "cdn.example-audio.com",
        ])
        #expect(result.stitchNetwork == .podtrac)
        #expect(result.daiExpected == true)
        #expect(result.matchedHost == "dts.podtrac.com")
    }

    @Test("pscrb.fm-only chain classifies as podscribe")
    func podscribeOnly() {
        let result = DAIStitchClassifier.classify(redirectChainHosts: ["pscrb.fm", "cdn.example.com"])
        #expect(result.stitchNetwork == .podscribe)
        #expect(result.daiExpected == true)
    }

    @Test("claritas host (clrtpod)")
    func claritasHost() {
        let result = DAIStitchClassifier.classify(redirectChainHosts: ["ap.clrtpod.com", "cdn.example.com"])
        #expect(result.stitchNetwork == .claritas)
        #expect(result.daiExpected == true)
    }

    @Test("libsyn-only chain expects DAI")
    func libsynOnly() {
        let result = DAIStitchClassifier.classify(redirectChainHosts: ["traffic.libsyn.com"])
        #expect(result.stitchNetwork == .libsyn)
        #expect(result.daiExpected == true)
    }

    // MARK: - Priority ordering

    @Test("megaphone outranks libsyn when both present regardless of chain order")
    func megaphoneOutranksLibsyn() {
        // libsyn appears first in the chain, but megaphone is the more
        // DAI-indicative network and must win.
        let result = DAIStitchClassifier.classify(redirectChainHosts: [
            "traffic.libsyn.com", "dcs.megaphone.fm",
        ])
        #expect(result.stitchNetwork == .megaphone)
        #expect(result.daiExpected == true)
    }

    @Test("stitcher (adswizz) outranks routing (podtrac)")
    func adswizzOutranksPodtrac() {
        let result = DAIStitchClassifier.classify(redirectChainHosts: [
            "dts.podtrac.com", "stitcher.adswizz.com",
        ])
        #expect(result.stitchNetwork == .adswizz)
    }

    // MARK: - Conservative: unknown / clean / empty

    @Test("clean direct-CDN chain (no known stitch host) is not DAI-expected")
    func cleanChainUnknown() {
        let result = DAIStitchClassifier.classify(redirectChainHosts: [
            "media.example.com", "cdn.cloudprovider.net",
        ])
        #expect(result.stitchNetwork == .unknown)
        #expect(result.daiExpected == false)
        #expect(result.matchedHost == nil)
    }

    @Test("empty chain is unknown / not DAI-expected")
    func emptyChain() {
        let result = DAIStitchClassifier.classify(redirectChainHosts: [])
        #expect(result == .unknown)
        #expect(result.daiExpected == false)
    }

    @Test("chain of only empty/whitespace hosts is unknown")
    func blankHostsChain() {
        let result = DAIStitchClassifier.classify(redirectChainHosts: ["", "   "])
        #expect(result.stitchNetwork == .unknown)
        #expect(result.daiExpected == false)
    }

    // MARK: - Normalization

    @Test("classification is case-insensitive and trims whitespace")
    func caseAndWhitespace() {
        let result = DAIStitchClassifier.classify(redirectChainHosts: ["  DCS.MEGAPHONE.FM  "])
        #expect(result.stitchNetwork == .megaphone)
        #expect(result.daiExpected == true)
    }

    // MARK: - Determinism

    @Test("classification is deterministic across repeated calls")
    func deterministic() {
        let hosts = ["pscrb.fm", "dts.podtrac.com", "mgln.ai", "traffic.libsyn.com"]
        let a = DAIStitchClassifier.classify(redirectChainHosts: hosts)
        let b = DAIStitchClassifier.classify(redirectChainHosts: hosts)
        #expect(a == b)
        #expect(a.stitchNetwork == .megaphone)
    }
}
