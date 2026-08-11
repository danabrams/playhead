// EvidenceCatalogLexiconAbsaTests.swift
// playhead-absa: the lexicon terms added to `EvidenceCatalogBuilder` so the
// DE0784D8 mid-roll pod stops being invisible to the catalog — `.ai/.co/.fm/.tv`
// URLs and their spoken forms, the CTA "head to", and the disclosure
// "our sponsor called".
//
// Every literal in this file is verbatim device-transcript text. The positive
// cases are real sponsor reads measured over 94,692 `transcript_chunks` rows
// (31 assets, fast + final passes) from the 2026-08-02 device pull; the
// negative cases are the ONLY non-ad occurrences that measurement found, so
// they are the precision cost this change is allowed to have and no more.
//
// The invariant that governs the whole change (`feedback_lexical_as_attention`):
// a wider lexicon may widen what is LOOKED AT and must not widen what is
// CLAIMED. `podAnchorsProduceNoDecodedSpan` is where that is enforced.

import Foundation
import Testing

@testable import Playhead

@Suite("EvidenceCatalogBuilder lexicon — playhead-absa")
struct EvidenceCatalogLexiconAbsaTests {

    // MARK: - Helpers

    /// One chunk = one atom, positional ordinals in time order — the same
    /// shape `TranscriptAtomizer.atomize` produces.
    private func atoms(_ chunks: [(Double, Double, String)]) -> [TranscriptAtom] {
        chunks.enumerated().map { ordinal, chunk in
            TranscriptAtom(
                atomKey: TranscriptAtomKey(
                    analysisAssetId: "absa-asset",
                    transcriptVersion: "absa",
                    atomOrdinal: ordinal
                ),
                contentHash: "absa-\(ordinal)",
                startTime: chunk.0,
                endTime: chunk.1,
                text: chunk.2,
                chunkIndex: ordinal
            )
        }
    }

    private func catalog(_ chunks: [(Double, Double, String)]) -> EvidenceCatalog {
        EvidenceCatalogBuilder.build(
            atoms: atoms(chunks),
            analysisAssetId: "absa-asset",
            transcriptVersion: "absa"
        )
    }

    private func texts(_ catalog: EvidenceCatalog, _ category: EvidenceCategory) -> Set<String> {
        Set(catalog.entries(for: category).map(\.normalizedText))
    }

    // MARK: - New TLDs, positive

    // A builder that matched `.ai` only inside a longer literal (say, only
    // "head to X.ai") would fail this: each case here is a BARE mention in its
    // own atom, which is how the ASR actually chunked them on device.
    @Test("A .ai domain is catalogued as a url, bare and spoken, from real device text")
    func aiDomainIsCatalogued() {
        let bare = catalog([(0, 2, "whisperflow.ai")])
        #expect(texts(bare, .url).contains("whisperflow.ai"))

        let netsuite = catalog([(0, 2, "netsweet.ai"), (2, 4, "slash"), (4, 6, "Bartlett.")])
        #expect(texts(netsuite, .url).contains("netsweet.ai"))

        // The spoken/spelled-out form, verbatim from the Jerry insurance read.
        let spoken = catalog([(0, 3, "That's"), (3, 6, "J-E-R-R-Y dot AI"), (6, 8, "slash")])
        #expect(texts(spoken, .url).contains("y dot ai"))
    }

    // The `.co` pattern must survive being a strict prefix of `.com`. A naive
    // `\b\w+\.co` without the trailing boundary would report "betterhelp.co"
    // here and this test would catch it.
    @Test("A .co domain is catalogued, and .com is NOT truncated to .co")
    func coDomainIsCataloguedAndComIsNotTruncated() {
        let coUK = catalog([(0, 4, "marketreach.co.uk.")])
        #expect(texts(coUK, .url).contains("marketreach.co"))

        let dotCom = catalog([(0, 4, "betterhelp.com")])
        #expect(texts(dotCom, .url).contains("betterhelp.com"))
        #expect(!texts(dotCom, .url).contains("betterhelp.co"))
    }

    // .fm/.tv fired ZERO times in the measured corpus, so this is the only
    // evidence that they work at all. Recorded as such: it proves the pattern
    // compiles and matches, not that the term earns its keep.
    @Test("UNMEASURED IN CORPUS: .fm and .tv domains are catalogued when they occur")
    func fmAndTvDomainsAreCatalogued() {
        #expect(texts(catalog([(0, 4, "Go to trypodcast.fm slash offer")]), .url)
            .contains("trypodcast.fm"))
        #expect(texts(catalog([(0, 4, "watch it at examplenetwork.tv")]), .url)
            .contains("examplenetwork.tv"))
    }

    // A `.ai` URL that anchors but seeds no brand stem is the half-fix this
    // guards: `brandStem`'s separator list is a SECOND list that has to be
    // kept in step with the patterns, and nothing but a test couples them.
    @Test("A .ai domain seeds a brand stem, so the brand corroborates like a .com one does")
    func aiDomainSeedsBrandStem() {
        let built = catalog([
            (0, 2, "Whisperflow"),
            (2, 4, "is 4 times faster than typing."),
            (4, 6, "whisperflow.ai"),
        ])
        #expect(texts(built, .brandSpan).contains("whisperflow"))
    }

    // MARK: - "head to", positive and the measured precision cost

    // The gate is the whole reason this term is admissible, so both halves are
    // asserted. An implementation that made ctaPhrase a GLOBAL category would
    // pass the first half and fail the second.
    @Test("'head to' is catalogued inside commercial context")
    func headToIsCataloguedInCommercialContext() {
        let built = catalog([
            (0, 3, "So if you'd like to join them, sign up at"),
            (3, 5, "pipedrive.com"),
            (5, 7, "slash CEO."),
            (7, 9, "Head to"),
            (9, 11, "pipedrive.com"),
        ])
        #expect(texts(built, .ctaPhrase).contains("head to"))
    }

    // The two non-ad occurrences measurement found in 94,692 chunks. Both are
    // ordinary speech and both must stay out of the catalog. A change that
    // promoted ctaPhrase to an anchor category — thereby making it feed
    // `commercialContextOrdinals` and gate itself in — would fail here.
    @Test("The two measured non-ad 'head to' uses are NOT catalogued (no commercial context)")
    func ordinaryHeadToUsesAreNotCatalogued() {
        let gym = catalog([
            (0, 5, "I can have a long coffee with my boys, have breakfast, head to the gym."),
            (5, 8, "And then I always know that when I do have food, my performance is better."),
        ])
        #expect(gym.entries.isEmpty)

        let headToToe = catalog([
            (0, 3, "get stretched head to toe,"),
            (3, 6, "and that is what recovery actually looks like."),
        ])
        #expect(headToToe.entries.isEmpty)
    }

    // MARK: - "our sponsor called", positive

    // A disclosure is an ANCHOR category, so this one is unconditional — which
    // is exactly why the phrase is the narrow "our sponsor called" and not the
    // wider "sponsor called". Both were measured; both had the same hit set.
    @Test("'our sponsor called' is catalogued as a disclosure, unconditionally")
    func ourSponsorCalledIsCatalogued() {
        let built = catalog([(0, 3, "of our sponsor called Whisper")])
        #expect(texts(built, .disclosurePhrase).contains("our sponsor called"))
    }

    // MARK: - Attention, not verdict

    // THE governing invariant. Feed the real WhisperFlow creative through
    // catalog -> projector -> decoder and require that three fresh anchors
    // produce zero claimed spans, because every run they form is under
    // `DecoderConstants.minDurationSeconds`.
    //
    // An implementation that ALSO relaxed the 5 s floor, or that widened a
    // single anchored atom to a minimum width, would pass every other test in
    // this file and fail this one. That is the failure this bead is scoped to
    // forbid.
    @Test("The new anchors widen what is looked at, not what is claimed: still zero decoded spans")
    func podAnchorsProduceNoDecodedSpan() async throws {
        // Verbatim device chunks spanning WhisperFlow's disclosure and CTA.
        let chunks: [(Double, Double, String)] = [
            (2838.18, 2839.26, "I've got 60"),
            (2839.26, 2839.68, "seconds and"),
            (2839.68, 2841.0, "I'm going to show you how much I can get"),
            (2841.0, 2841.66, "done because"),
            (2841.66, 2843.88, "of our sponsor called Whisper"),
            (2843.88, 2844.48, "Flow."),
            (2891.94, 2894.22, "So if you want to give it a go, all you have to do is head to"),
            (2894.22, 2896.08, "whisperflow.ai"),
            (2896.08, 2896.62, "slash"),
            (2896.62, 2896.86, "Stephen"),
            (2896.86, 2897.94, "to download it today."),
        ]
        let built = catalog(chunks)

        // Attention DID widen: all three terms fired.
        #expect(texts(built, .disclosurePhrase).contains("our sponsor called"))
        #expect(texts(built, .ctaPhrase).contains("head to"))
        #expect(texts(built, .url).contains("whisperflow.ai"))

        let evidence = await AtomEvidenceProjector().project(
            regions: [],
            catalog: built,
            atoms: atoms(chunks),
            correctionMaskProvider: NoCorrectionMaskProvider()
        )
        #expect(evidence.filter(\.isAnchored).count == 3,
                "three atoms anchored — the disclosure, the CTA and the URL")

        let spans = MinimalContiguousSpanDecoder().decode(
            atoms: evidence,
            assetId: "absa-asset"
        )
        #expect(
            spans.isEmpty,
            """
            playhead-absa is a LEXICON change only. The anchored runs here are \
            2.22 s (disclosure) and 4.14 s (CTA + URL, contiguous), both under \
            DecoderConstants.minDurationSeconds = \(DecoderConstants.minDurationSeconds). \
            Decoding a span from them means the CLAIM widened, which is \
            playhead-hp70 / j4wi item 3, not this bead. Got: \
            \(spans.map { ($0.startTime, $0.endTime) }).
            """
        )
    }
}
