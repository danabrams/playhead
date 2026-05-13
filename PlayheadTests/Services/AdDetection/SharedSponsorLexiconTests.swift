import Foundation
import Testing
@testable import Playhead

@Suite("SharedSponsorLexiconTests")
struct SharedSponsorLexiconTests {

    private let signer = DeterministicSharedSponsorLexiconSigner(secret: "phase-12-test-secret")

    @Test("signed artifact validates and canonicalization is deterministic")
    func validatesSignedArtifactAndCanonicalizesDeterministically() throws {
        let entriesA = [
            entry(id: "b", name: "BetterHelp", aliases: ["better help"]),
            entry(id: "a", name: "Squarespace", aliases: ["square space"]),
        ]
        let entriesB = entriesA.reversed()

        let artifactA = try signedArtifact(entries: Array(entriesA))
        let artifactB = try signedArtifact(entries: Array(entriesB))
        let validator = SharedSponsorLexiconValidator(verifier: signer)

        _ = try validator.decodeAndValidate(artifactA.encodedData())
        _ = try validator.decodeAndValidate(artifactB.encodedData())
        #expect(try artifactA.canonicalPayloadData() == artifactB.canonicalPayloadData())
        #expect(artifactA.manifest.contentDigest == artifactB.manifest.contentDigest)
    }

    @Test("decoder rejects incompatible schema, invalid signature, malformed entries, unknown fields, and private data")
    func decoderFailsClosed() throws {
        let validator = SharedSponsorLexiconValidator(verifier: signer)
        let valid = try signedArtifact(entries: [entry(name: "Squarespace")])
        let validData = try valid.encodedData()

        #expect(throws: SharedSponsorLexiconError.incompatibleSchema(99)) {
            var artifact = valid
            artifact = SharedSponsorLexiconArtifact(
                schemaVersion: 99,
                artifactVersion: artifact.artifactVersion,
                manifest: artifact.manifest,
                entries: artifact.entries,
                signature: artifact.signature
            )
            _ = try validator.decodeAndValidate(artifact.encodedData())
        }

        #expect(throws: SharedSponsorLexiconError.invalidSignature) {
            let tampered = String(data: validData, encoding: .utf8)!
                .replacingOccurrences(of: valid.signature.value, with: String(repeating: "0", count: valid.signature.value.count))
            _ = try validator.decodeAndValidate(Data(tampered.utf8))
        }

        #expect(throws: SharedSponsorLexiconError.malformedArtifact("non-canonical entries")) {
            _ = try validator.decodeAndValidate(
                injectEntryField("canonicalName", value: " Squarespace ", into: validData)
            )
        }

        #expect(throws: SharedSponsorLexiconError.invalidEntry("empty canonicalName")) {
            let malformed = try signedArtifact(entries: [entry(name: " ")])
            _ = try validator.decodeAndValidate(malformed.encodedData())
        }

        #expect(throws: SharedSponsorLexiconError.privacyViolation("entries.[0].version")) {
            _ = try validator.decodeAndValidate(
                injectEntryField("version", value: "\"raw transcript quote\"", into: validData)
            )
        }

        #expect(throws: SharedSponsorLexiconError.privacyViolation("artifactVersion")) {
            _ = try validator.decodeAndValidate(
                injectTopLevelField("artifactVersion", value: "\"raw transcript quote\"", into: validData)
            )
        }

        #expect(throws: SharedSponsorLexiconError.privacyViolation("signature.keyId")) {
            _ = try validator.decodeAndValidate(
                injectNestedField("signature", key: "keyId", value: "user-ghi789", into: validData)
            )
        }

        #expect(throws: SharedSponsorLexiconError.privacyViolation("signature.value")) {
            _ = try validator.decodeAndValidate(
                injectNestedField("signature", key: "value", value: "transcript snippet from signer", into: validData)
            )
        }

        #expect(throws: SharedSponsorLexiconError.privacyViolation("manifest.createdAt")) {
            _ = try validator.decodeAndValidate(
                injectNestedField("manifest", key: "createdAt", value: "host said use code podcast", into: validData)
            )
        }

        #expect(throws: SharedSponsorLexiconError.unknownField("entries[0].futureField")) {
            _ = try validator.decodeAndValidate(injectEntryField("futureField", value: "public", into: validData))
        }

        for (field, value) in forbiddenFieldSamples() {
            #expect(throws: SharedSponsorLexiconError.privacyViolation("entries.[0].\(field)")) {
                _ = try validator.decodeAndValidate(injectEntryField(field, value: value, into: validData))
            }
        }

        for forbiddenValue in forbiddenPublicTermSamples() {
            #expect(throws: SharedSponsorLexiconError.privacyViolation("entries.[0].aliases.[0]")) {
                _ = try validator.decodeAndValidate(
                    injectEntryField("aliases", value: [forbiddenValue], into: validData)
                )
            }
        }
    }

    @Test("device client verifies before promotion, rolls back on failures, and loads cache offline")
    func clientCacheRollbackAndOfflineLoad() async throws {
        let cacheDir = temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: cacheDir) }

        let v1 = try signedArtifact(
            version: "2026.05.12.1",
            entries: [entry(id: "sq", name: "Squarespace")]
        )
        let v2 = try signedArtifact(
            version: "2026.05.12.2",
            entries: [entry(id: "bh", name: "BetterHelp")]
        )
        let transport = MutableSharedSponsorLexiconTransport(data: try v1.encodedData())
        let client = SharedSponsorLexiconClient(
            transport: transport,
            validator: SharedSponsorLexiconValidator(verifier: signer),
            cacheDirectory: cacheDir
        )
        let emptyCacheLoad = await client.loadFromCache()
        #expect(emptyCacheLoad == nil)
        #expect(await client.diagnostics.validationStatus == .notLoaded)

        let loadedV1 = await client.refresh()
        #expect(loadedV1?.artifactVersion == "2026.05.12.1")

        let tampered = String(data: try v2.encodedData(), encoding: .utf8)!
            .replacingOccurrences(of: v2.signature.value, with: String(repeating: "f", count: v2.signature.value.count))
        await transport.update(data: Data(tampered.utf8))
        let afterBadRefresh = await client.refresh()
        #expect(afterBadRefresh?.artifactVersion == "2026.05.12.1")
        #expect(await client.diagnostics.validationStatus == .invalidSignature)

        await transport.update(data: try removeTopLevelField("entries", from: try v2.encodedData()))
        let afterMalformedRefresh = await client.refresh()
        #expect(afterMalformedRefresh?.artifactVersion == "2026.05.12.1")
        #expect(await client.diagnostics.validationStatus == .malformedArtifact)

        await transport.update(data: try injectEntryField("canonicalName", value: " ", into: try v2.encodedData()))
        let afterInvalidEntryRefresh = await client.refresh()
        #expect(afterInvalidEntryRefresh?.artifactVersion == "2026.05.12.1")
        #expect(await client.diagnostics.validationStatus == .malformedArtifact)

        try Data(tampered.utf8).write(
            to: cacheDir.appendingPathComponent("shared-sponsor-lexicon-lkg.json"),
            options: [.atomic]
        )
        let afterCorruptCacheLoad = await client.loadFromCache()
        #expect(afterCorruptCacheLoad?.artifactVersion == "2026.05.12.1")
        #expect(await client.diagnostics.validationStatus == .invalidSignature)
        try v1.encodedData().write(
            to: cacheDir.appendingPathComponent("shared-sponsor-lexicon-lkg.json"),
            options: [.atomic]
        )

        let coldRefreshClient = SharedSponsorLexiconClient(
            transport: MutableSharedSponsorLexiconTransport(data: Data(tampered.utf8)),
            validator: SharedSponsorLexiconValidator(verifier: signer),
            cacheDirectory: cacheDir
        )
        let coldFallback = await coldRefreshClient.refresh()
        #expect(coldFallback?.artifactVersion == "2026.05.12.1")
        #expect(await coldRefreshClient.diagnostics.validationStatus == .invalidSignature)

        let offlineClient = SharedSponsorLexiconClient(
            transport: ClosureSharedSponsorLexiconTransport { throw URLError(.notConnectedToInternet) },
            validator: SharedSponsorLexiconValidator(verifier: signer),
            cacheDirectory: cacheDir
        )
        let offline = await offlineClient.loadFromCache()
        #expect(offline?.artifactVersion == "2026.05.12.1")
        #expect(await offlineClient.diagnostics.validationStatus == .valid)
    }

    private actor MutableSharedSponsorLexiconTransport: SharedSponsorLexiconTransport {
        private var data: Data

        init(data: Data) {
            self.data = data
        }

        func update(data: Data) {
            self.data = data
        }

        func fetchArtifactData() async throws -> Data {
            data
        }
    }

    private struct UnsafeSignatureMetadataSigner: SharedSponsorLexiconSigning, SharedSponsorLexiconVerifying {
        func sign(data: Data, keyId: String) throws -> SharedSponsorLexiconSignature {
            SharedSponsorLexiconSignature(
                keyId: keyId,
                algorithm: "transcript snippet from signer",
                value: "test-signature"
            )
        }

        func verify(signature: SharedSponsorLexiconSignature, data: Data) throws -> Bool {
            true
        }
    }

    @Test("consumption disablement does not weaken local-only detection")
    func disabledConsumptionLeavesLocalLexiconUnchanged() throws {
        var settings = SharedSponsorLexiconSettings()
        settings.disableConsumption()

        let local = SponsorKnowledgeEntry(
            podcastId: "pod",
            entityType: .sponsor,
            entityValue: "LocalSponsor",
            state: .active,
            confirmationCount: 2
        )
        let shared = try verifiedArtifact(
            signedArtifact(entries: [entry(name: "SharedSponsor", aliases: ["shared sponsor"])])
        )
        let lexicon = CompiledSponsorLexicon(
            localEntries: [local],
            sharedArtifact: shared,
            settings: settings
        )
        let scanner = LexicalScanner(compiledLexicon: lexicon)

        let localHits = scanner.scanChunk(chunk("LocalSponsor appears here"))
        let sharedHits = scanner.scanChunk(chunk("shared sponsor appears here"))

        #expect(lexicon.entryCount == 1)
        #expect(lexicon.sharedEntryCount == 0)
        #expect(localHits.contains { $0.matchedText.lowercased().contains("localsponsor") })
        #expect(!sharedHits.contains { $0.matchedText.lowercased().contains("shared") })
    }

    @Test("verified shared terms participate in candidates but stay non skip eligible and local overrides suppress them")
    func mergeSharedTermsWithQuarantineAndOverrides() throws {
        let shared = try verifiedArtifact(signedArtifact(entries: [
            entry(
                id: "sq",
                name: "Squarespace",
                aliases: ["square space"],
                vanityURLs: ["squarespace.com/podcast"],
                ctaPatternDescriptors: ["use code podcast"],
                disclosurePhrases: ["brought to you by squarespace"]
            ),
            entry(id: "blocked", name: "BlockedSponsor", aliases: ["blocked sponsor"]),
            entry(id: "corrected", name: "CorrectedSponsor", aliases: ["corrected sponsor"]),
            entry(id: "negative-memory", name: "NegativeMemorySponsor", aliases: ["negative memory sponsor"]),
            entry(id: "url-only", name: "urlsponsor.com/podcast"),
        ]))
        let blocked = SponsorKnowledgeEntry(
            podcastId: "pod",
            entityType: .sponsor,
            entityValue: "Blocked Sponsor",
            normalizedValue: "blocked sponsor",
            state: .blocked,
            aliases: []
        )
        let corrected = SponsorKnowledgeEntry(
            podcastId: "pod",
            entityType: .sponsor,
            entityValue: "Corrected Sponsor",
            normalizedValue: "corrected sponsor",
            state: .active,
            confirmationCount: 2,
            aliases: [],
            metadata: ["corrected": "true"]
        )
        let blockedURLSponsor = SponsorKnowledgeEntry(
            podcastId: "pod",
            entityType: .sponsor,
            entityValue: "URL Sponsor",
            normalizedValue: "url sponsor",
            state: .blocked,
            aliases: []
        )
        let lexicon = CompiledSponsorLexicon(
            localEntries: [blocked, corrected, blockedURLSponsor],
            sharedArtifact: shared,
            additionalLocalOverrideTerms: ["negative memory sponsor"]
        )
        let scanner = LexicalScanner(compiledLexicon: lexicon)

        #expect(lexicon.entryCount == 0)
        #expect(lexicon.sharedEntryCount == 1)
        #expect(lexicon.skipEligibleEntryCount == 0)
        #expect(lexicon.patterns.isEmpty)
        #expect(!lexicon.sharedCandidatePatterns.isEmpty)
        #expect(shared.quarantineEntries(podcastId: "pod")[0].state == .quarantined)
        #expect(shared.quarantineEntries(podcastId: "pod")[0].metadata?["origin"] == "shared")

        for text in [
            "square space is sponsoring today",
            "visit squarespace.com/podcast",
            "use code podcast for the offer",
            "brought to you by squarespace",
        ] {
            let hits = scanner.scanChunk(chunk(text))
            #expect(!hits.filter { $0.category == .transitionMarker }.isEmpty, "Expected shared hit for \(text)")
        }
        let blockedHits = scanner.scanChunk(chunk("blocked sponsor is mentioned"))
        #expect(!blockedHits.contains { $0.matchedText.lowercased().contains("blocked") })
        let correctedHits = scanner.scanChunk(chunk("corrected sponsor is mentioned"))
        #expect(!correctedHits.contains { $0.matchedText.lowercased().contains("corrected") })
        let negativeMemoryHits = scanner.scanChunk(chunk("negative memory sponsor is mentioned"))
        #expect(!negativeMemoryHits.contains { $0.matchedText.lowercased().contains("negative") })

        let counts = CompiledSponsorLexicon.sharedMergeCounts(
            localEntries: [blocked, corrected, blockedURLSponsor],
            sharedArtifact: shared,
            additionalLocalOverrideTerms: ["negative memory sponsor"]
        )
        #expect(counts.sharedCandidate == 1)
        #expect(counts.sharedSuppressedByLocalOverride == 4)

        let sharedOnlyCandidates = scanner.scan(
            chunks: [chunk("square space is mentioned once")],
            analysisAssetId: "asset-shared-lexicon"
        )
        #expect(sharedOnlyCandidates.isEmpty, "Shared-only single hits must not bypass local confirmation")

        let sharedOnlyCandidate = try #require(scanner.scan(
            chunks: [chunk("square space then squarespace")],
            analysisAssetId: "asset-shared-lexicon"
        ).first)
        #expect(sharedOnlyCandidate.categories == [.transitionMarker])
        #expect(!AutoSkipPrecisionGate.isStrongLexicalAdPhrase(categories: sharedOnlyCandidate.categories))
    }

    @Test("local generic descriptor overrides remove only that shared pattern")
    func localGenericDescriptorOverrideDoesNotSuppressSponsorIdentity() throws {
        let shared = try verifiedArtifact(signedArtifact(entries: [
            entry(
                id: "sq",
                name: "Squarespace",
                ctaPatternDescriptors: ["use code podcast"]
            ),
        ]))
        let blockedCTA = SponsorKnowledgeEntry(
            podcastId: "pod",
            entityType: .cta,
            entityValue: "Use Code Podcast",
            normalizedValue: "use code podcast",
            state: .blocked
        )
        let lexicon = CompiledSponsorLexicon(
            localEntries: [blockedCTA],
            sharedArtifact: shared
        )
        let scanner = LexicalScanner(compiledLexicon: lexicon)

        #expect(lexicon.sharedEntryCount == 1)
        #expect(CompiledSponsorLexicon.sharedMergeCounts(
            localEntries: [blockedCTA],
            sharedArtifact: shared
        ) == SharedSponsorLexiconMergeCounts(
            localActive: 0,
            sharedCandidate: 1,
            sharedSuppressedByLocalOverride: 0
        ))

        let sponsorHits = scanner.scanChunk(chunk("Squarespace has an offer"))
        #expect(sponsorHits.contains {
            $0.category == .transitionMarker
                && $0.matchedText.lowercased() == "squarespace"
        })

        let descriptorHits = scanner.scanChunk(chunk("use code podcast"))
        #expect(!descriptorHits.contains {
            $0.category == .transitionMarker
                && $0.matchedText.lowercased().contains("use code")
        })
    }

    @Test("local generic descriptor override does not identity-suppress misplaced shared aliases")
    func localGenericDescriptorOverrideDoesNotSuppressAliasIdentity() throws {
        let shared = try verifiedArtifact(signedArtifact(entries: [
            entry(
                id: "sq",
                name: "Squarespace",
                aliases: ["use code podcast"]
            ),
        ]))
        let blockedCTA = SponsorKnowledgeEntry(
            podcastId: "pod",
            entityType: .cta,
            entityValue: "Use Code Podcast",
            normalizedValue: "use code podcast",
            state: .blocked
        )
        let lexicon = CompiledSponsorLexicon(
            localEntries: [blockedCTA],
            sharedArtifact: shared
        )
        let scanner = LexicalScanner(compiledLexicon: lexicon)

        #expect(lexicon.sharedEntryCount == 1)
        #expect(CompiledSponsorLexicon.sharedMergeCounts(
            localEntries: [blockedCTA],
            sharedArtifact: shared
        ) == SharedSponsorLexiconMergeCounts(
            localActive: 0,
            sharedCandidate: 1,
            sharedSuppressedByLocalOverride: 0
        ))

        let sponsorHits = scanner.scanChunk(chunk("Squarespace has an offer"))
        #expect(sponsorHits.contains {
            $0.category == .transitionMarker
                && $0.matchedText.lowercased() == "squarespace"
        })

        let aliasHits = scanner.scanChunk(chunk("use code podcast"))
        #expect(!aliasHits.contains {
            $0.category == .transitionMarker
                && $0.matchedText.lowercased().contains("use code")
        })
    }

    @Test("additional generic descriptor overrides remove only that shared pattern")
    func additionalGenericDescriptorOverrideDoesNotSuppressSponsorIdentity() throws {
        let shared = try verifiedArtifact(signedArtifact(entries: [
            entry(
                id: "sq",
                name: "Squarespace",
                ctaPatternDescriptors: ["use code podcast"]
            ),
        ]))
        let lexicon = CompiledSponsorLexicon(
            localEntries: [],
            sharedArtifact: shared,
            additionalLocalOverrideTerms: ["use code podcast"]
        )
        let scanner = LexicalScanner(compiledLexicon: lexicon)

        #expect(lexicon.sharedEntryCount == 1)
        #expect(CompiledSponsorLexicon.sharedMergeCounts(
            localEntries: [],
            sharedArtifact: shared,
            additionalLocalOverrideTerms: ["use code podcast"]
        ) == SharedSponsorLexiconMergeCounts(
            localActive: 0,
            sharedCandidate: 1,
            sharedSuppressedByLocalOverride: 0
        ))

        let sponsorHits = scanner.scanChunk(chunk("Squarespace has an offer"))
        #expect(sponsorHits.contains {
            $0.category == .transitionMarker
                && $0.matchedText.lowercased() == "squarespace"
        })

        let descriptorHits = scanner.scanChunk(chunk("use code podcast"))
        #expect(!descriptorHits.contains {
            $0.category == .transitionMarker
                && $0.matchedText.lowercased().contains("use code")
        })
    }

    @Test("shared merge consumes only validator-issued verified artifacts")
    func sharedMergeRequiresVerifiedArtifact() throws {
        let signed = try signedArtifact(entries: [entry(id: "sq", name: "Squarespace")])
        let verified = try verifiedArtifact(signed)
        let lexicon = CompiledSponsorLexicon(localEntries: [], sharedArtifact: verified)
        let scanner = LexicalScanner(compiledLexicon: lexicon)

        #expect(lexicon.sharedEntryCount == 1)
        #expect(scanner.scanChunk(chunk("squarespace")).contains {
            $0.category == .transitionMarker
                && $0.matchedText.lowercased() == "squarespace"
        })

        let tampered = SharedSponsorLexiconArtifact(
            schemaVersion: signed.schemaVersion,
            artifactVersion: signed.artifactVersion,
            manifest: signed.manifest,
            entries: [entry(id: "bh", name: "BetterHelp")],
            signature: signed.signature
        )
        #expect(throws: SharedSponsorLexiconError.malformedArtifact("content digest mismatch")) {
            _ = try SharedSponsorLexiconValidator(verifier: signer).validate(tampered)
        }
    }

    @Test("active shared-origin local entries require local confirmation for boosted skip eligibility")
    func sharedOriginActiveEntriesRequireLocalConfirmation() {
        let unconfirmedShared = SponsorKnowledgeEntry(
            podcastId: "pod",
            entityType: .sponsor,
            entityValue: "UnconfirmedShared",
            state: .active,
            confirmationCount: 0,
            metadata: ["origin": "shared"]
        )
        let locallyConfirmedShared = SponsorKnowledgeEntry(
            podcastId: "pod",
            entityType: .sponsor,
            entityValue: "ConfirmedShared",
            state: .active,
            confirmationCount: 1,
            metadata: ["origin": "shared"]
        )
        let lexicon = CompiledSponsorLexicon(entries: [unconfirmedShared, locallyConfirmedShared])
        let scanner = LexicalScanner(compiledLexicon: lexicon)

        #expect(lexicon.entryCount == 1)
        #expect(lexicon.skipEligibleEntryCount == 1)
        #expect(scanner.scanChunk(chunk("UnconfirmedShared appears here")).isEmpty)

        let confirmedHits = scanner.scanChunk(chunk("ConfirmedShared appears here"))
        #expect(confirmedHits.contains {
            $0.category == .sponsor
                && $0.matchedText.lowercased().contains("confirmedshared")
                && $0.weight == 1.5
        })
    }

    @Test("contribution exporter is opt-in and exports only eligible public facts")
    func contributionExporterEligibility() throws {
        let active = SponsorKnowledgeEntry(
            podcastId: "pod",
            entityType: .sponsor,
            entityValue: "Squarespace",
            state: .active,
            confirmationCount: 3,
            rollbackCount: 0,
            aliases: ["square space"]
        )
        let highRollback = SponsorKnowledgeEntry(
            podcastId: "pod",
            entityType: .sponsor,
            entityValue: "Rollback",
            state: .active,
            confirmationCount: 2,
            rollbackCount: 2
        )
        let underConfirmed = SponsorKnowledgeEntry(
            podcastId: "pod",
            entityType: .sponsor,
            entityValue: "UnderConfirmed",
            state: .active,
            confirmationCount: 1,
            rollbackCount: 0
        )
        let sharedOrigin = SponsorKnowledgeEntry(
            podcastId: "pod",
            entityType: .sponsor,
            entityValue: "Shared",
            state: .active,
            confirmationCount: 2,
            metadata: ["origin": "shared"]
        )
        let quarantinedSharedOrigin = SponsorKnowledgeEntry(
            podcastId: "pod",
            entityType: .sponsor,
            entityValue: "QuarantinedShared",
            state: .quarantined,
            confirmationCount: 2,
            metadata: ["origin": "shared"]
        )
        let corrected = SponsorKnowledgeEntry(
            podcastId: "pod",
            entityType: .sponsor,
            entityValue: "Corrected",
            state: .active,
            confirmationCount: 2,
            metadata: ["corrected": "true"]
        )

        #expect(try SharedSponsorContributionExporter(settings: SharedSponsorLexiconSettings()).export(entries: [active]) == nil)

        let settings = SharedSponsorLexiconSettings(consumptionEnabled: true, contributionOptedIn: true)
        let exporter = SharedSponsorContributionExporter(settings: settings)
        let entries = [active, highRollback, underConfirmed, sharedOrigin, quarantinedSharedOrigin, corrected]
        let payload = try #require(try exporter.export(entries: entries))
        let counts = exporter.eligibilityCounts(entries: entries)

        #expect(payload.entries.map(\.canonicalName) == ["Squarespace"])
        #expect(counts.eligible == 1)
        #expect(counts.excludedHighRollback == 1)
        #expect(counts.excludedUnderconfirmed == 1)
        #expect(counts.excludedSharedOrQuarantined == 2)
        #expect(counts.excludedConflicted == 1)
        _ = try payload.encodedData()
    }

    @Test("contribution exporter preserves type-specific public fields")
    func contributionExporterPreservesTypeSpecificFields() throws {
        let settings = SharedSponsorLexiconSettings(consumptionEnabled: true, contributionOptedIn: true)
        let exporter = SharedSponsorContributionExporter(settings: settings)
        let entries = [
            SponsorKnowledgeEntry(
                podcastId: "pod",
                entityType: .url,
                entityValue: "squarespace.com/podcast",
                state: .active,
                confirmationCount: 3,
                aliases: ["square.space/podcast"]
            ),
            SponsorKnowledgeEntry(
                podcastId: "pod",
                entityType: .cta,
                entityValue: "use code podcast",
                state: .active,
                confirmationCount: 3,
                aliases: ["enter code podcast"]
            ),
            SponsorKnowledgeEntry(
                podcastId: "pod",
                entityType: .disclosure,
                entityValue: "brought to you by squarespace",
                state: .active,
                confirmationCount: 3,
                aliases: ["sponsored by squarespace"]
            ),
        ]

        let payload = try #require(try exporter.export(entries: entries))
        let byType = Dictionary(uniqueKeysWithValues: payload.entries.map { ($0.type, $0) })

        #expect(byType[.url]?.vanityURLs == ["square.space/podcast", "squarespace.com/podcast"])
        #expect(byType[.url]?.aliases.isEmpty == true)
        #expect(byType[.cta]?.ctaPatternDescriptors == ["enter code podcast", "use code podcast"])
        #expect(byType[.cta]?.aliases.isEmpty == true)
        #expect(byType[.disclosure]?.disclosurePhrases == ["brought to you by squarespace", "sponsored by squarespace"])
        #expect(byType[.disclosure]?.aliases.isEmpty == true)
        _ = try payload.encodedData()
    }

    @Test("contribution privacy guard rejects every forbidden data class")
    func contributionPrivacyGuardRejectsForbiddenDataClasses() throws {
        try SharedSponsorPrivacyGuard.validatePublicTerms([
            "account-based marketing",
            "customer support platform",
            "listener-supported network",
            "subscription-based network",
            "profile builder",
            "device management platform",
        ])

        for (field, value) in forbiddenFieldSamples() {
            #expect(throws: SharedSponsorLexiconError.privacyViolation(field)) {
                let data = try JSONSerialization.data(withJSONObject: [field: value])
                try SharedSponsorPrivacyGuard.validateJSONData(data)
            }
        }

        #expect(throws: SharedSponsorLexiconError.privacyViolation("entries.[0].version")) {
            let payload = SharedSponsorContributionPayload(
                schemaVersion: SharedSponsorLexiconSchema.supportedVersion,
                exporterVersion: "test",
                entries: [
                    SharedSponsorLexiconEntry(
                        id: "unsafe-version",
                        canonicalName: "Sponsor",
                        version: "\"raw transcript quote\""
                    ),
                ]
            )
            _ = try payload.encodedData()
        }

        for forbiddenValue in forbiddenPublicTermSamples() {
            #expect(throws: SharedSponsorLexiconError.privacyViolation("entries.[0].aliases.[0]")) {
                let payload = SharedSponsorContributionPayload(
                    schemaVersion: SharedSponsorLexiconSchema.supportedVersion,
                    exporterVersion: "test",
                    entries: [
                        SharedSponsorLexiconEntry(
                            id: "unsafe-alias",
                            canonicalName: "Sponsor",
                            aliases: [forbiddenValue]
                        ),
                    ]
                )
                _ = try payload.encodedData()
            }
        }
    }

    @Test("contribution payload encoding canonicalizes public entries and rejects malformed entries")
    func contributionPayloadEncodingCanonicalizesEntries() throws {
        let payload = SharedSponsorContributionPayload(
            schemaVersion: SharedSponsorLexiconSchema.supportedVersion,
            exporterVersion: "test",
            entries: [
                entry(id: "b", name: "Squarespace", vanityURLs: ["Squarespace.com/Podcast"]),
                entry(id: "a", name: "BetterHelp", aliases: [" better help ", "Better Help"]),
            ]
        )

        let decoded = try JSONDecoder().decode(
            SharedSponsorContributionPayload.self,
            from: payload.encodedData()
        )

        #expect(decoded.entries.map(\.canonicalName) == ["BetterHelp", "Squarespace"])
        #expect(decoded.entries[0].aliases == ["Better Help", "better help"])
        #expect(decoded.entries[1].vanityURLs == ["squarespace.com/podcast"])

        let malformed = SharedSponsorContributionPayload(
            schemaVersion: SharedSponsorLexiconSchema.supportedVersion,
            exporterVersion: "test",
            entries: [entry(name: " ")]
        )
        #expect(throws: SharedSponsorLexiconError.invalidEntry("empty canonicalName")) {
            _ = try malformed.encodedData()
        }
    }

    @Test("central pipeline rejects unsafe payloads, dedupes deterministically, signs publish, and retains rollback artifact")
    func centralPublishPipeline() throws {
        var publisher = SharedSponsorLexiconPublisher()
        let payloadA = SharedSponsorContributionPayload(
            schemaVersion: SharedSponsorLexiconSchema.supportedVersion,
            exporterVersion: "test",
            entries: [
                entry(id: "sq-1", name: "Squarespace", aliases: ["Square Space"], vanityURLs: ["Squarespace.com/Podcast"]),
            ]
        )
        let payloadB = SharedSponsorContributionPayload(
            schemaVersion: SharedSponsorLexiconSchema.supportedVersion,
            exporterVersion: "test",
            entries: [
                entry(id: "sq-2", name: "squarespace", aliases: ["square space"], ctaPatternDescriptors: ["use code podcast"]),
                entry(id: "blocked", name: "BlockedSponsor"),
                entry(id: "blocked-url", name: "blockedsponsor.com/podcast"),
            ]
        )

        let equivalentEntries = [
            entry(id: "sq-url", name: "Square Space", vanityURLs: ["Squarespace.com/Podcast"]),
            entry(id: "sq-alias", name: "Squarespace", aliases: ["square space"], ctaPatternDescriptors: ["use code podcast"]),
        ]
        let dedupedA = try SharedSponsorLexiconPublisher.dedupe(entries: equivalentEntries)
        let dedupedB = try SharedSponsorLexiconPublisher.dedupe(entries: Array(equivalentEntries.reversed()))
        #expect(dedupedA == dedupedB)
        #expect(dedupedA.count == 1)
        #expect(dedupedA[0].canonicalName == "Squarespace")
        #expect(dedupedA[0].aliases == ["Square Space", "square space"])
        #expect(dedupedA[0].vanityURLs == ["squarespace.com/podcast"])
        #expect(dedupedA[0].ctaPatternDescriptors == ["use code podcast"])

        let first = try publisher.publish(
            contributions: [payloadB, payloadA],
            artifactVersion: "2026.05.12.1",
            createdAt: "2026-05-12",
            keyId: "phase12",
            signer: signer,
            blocklist: ["blocked sponsor"]
        )
        let second = try publisher.publish(
            contributions: [payloadA, payloadB],
            artifactVersion: "2026.05.12.2",
            createdAt: "2026-05-12",
            keyId: "phase12",
            signer: signer,
            blocklist: ["blocked sponsor"]
        )

        #expect(first.artifact.entries.count == 1)
        #expect(!first.artifact.entries.flatMap(\.publicTerms).contains { $0.contains("blockedsponsor") })
        #expect(first.artifact.entries[0].aliases == ["Square Space", "square space"])
        #expect(first.artifact.entries[0].vanityURLs == ["squarespace.com/podcast"])
        #expect(first.artifact.entries[0].ctaPatternDescriptors == ["use code podcast"])
        #expect(second.previousArtifact?.artifactVersion == "2026.05.12.1")

        let validator = SharedSponsorLexiconValidator(verifier: signer)
        _ = try validator.decodeAndValidate(second.artifact.encodedData())

        let unsafe = SharedSponsorContributionPayload(
            schemaVersion: SharedSponsorLexiconSchema.supportedVersion,
            exporterVersion: "test",
            entries: [entry(name: "Sponsor", aliases: ["episode-abc123"])]
        )
        #expect(throws: SharedSponsorLexiconError.privacyViolation("entries.[0].aliases.[0]")) {
            _ = try publisher.publish(
                contributions: [unsafe],
                artifactVersion: "2026.05.12.3",
                createdAt: "2026-05-12",
                keyId: "phase12",
                signer: signer
            )
        }

        let incompatibleContribution = SharedSponsorContributionPayload(
            schemaVersion: 99,
            exporterVersion: "test",
            entries: [entry(name: "Sponsor")]
        )
        #expect(throws: SharedSponsorLexiconError.incompatibleSchema(99)) {
            _ = try incompatibleContribution.encodedData()
        }

        let unsafeSignatureSigner = UnsafeSignatureMetadataSigner()
        #expect(throws: SharedSponsorLexiconError.privacyViolation("signature.algorithm")) {
            _ = try publisher.publish(
                contributions: [payloadA],
                artifactVersion: "2026.05.12.4",
                createdAt: "2026-05-12",
                keyId: "phase12",
                signer: unsafeSignatureSigner
            )
        }
    }

    @Test("central dedupe does not merge distinct sponsors that share generic descriptors")
    func centralDedupeKeepsDescriptorConflictsSeparate() throws {
        let entries = [
            entry(id: "sq", name: "Squarespace", ctaPatternDescriptors: ["use code podcast"]),
            entry(id: "bh", name: "BetterHelp", ctaPatternDescriptors: ["use code podcast"]),
        ]

        let deduped = try SharedSponsorLexiconPublisher.dedupe(entries: entries)

        #expect(deduped.map(\.canonicalName) == ["BetterHelp", "Squarespace"])
        #expect(deduped.allSatisfy { $0.aliases.isEmpty })
        #expect(deduped.allSatisfy { $0.ctaPatternDescriptors == ["use code podcast"] })

        let genericDescriptorBlocklist = try SharedSponsorLexiconPublisher.dedupe(
            entries: entries,
            blocklist: ["use code podcast"]
        )
        #expect(genericDescriptorBlocklist.map(\.canonicalName) == ["BetterHelp", "Squarespace"])

        let misplacedAliasEntries = [
            entry(id: "sq", name: "Squarespace", aliases: ["use code podcast"]),
            entry(id: "bh", name: "BetterHelp", aliases: ["use code podcast"]),
        ]
        let misplacedAliasDeduped = try SharedSponsorLexiconPublisher.dedupe(entries: misplacedAliasEntries)
        #expect(misplacedAliasDeduped.map(\.canonicalName) == ["BetterHelp", "Squarespace"])
        #expect(misplacedAliasDeduped.allSatisfy { $0.aliases == ["use code podcast"] })

        let misplacedAliasBlocklist = try SharedSponsorLexiconPublisher.dedupe(
            entries: misplacedAliasEntries,
            blocklist: ["use code podcast"]
        )
        #expect(misplacedAliasBlocklist.map(\.canonicalName) == ["BetterHelp", "Squarespace"])

        let vanitySubdomainEntries = [
            entry(id: "sq", name: "Squarespace", vanityURLs: ["podcast.squarespace.com/offer"]),
            entry(id: "bh", name: "BetterHelp", vanityURLs: ["podcast.betterhelp.com/offer"]),
        ]
        let vanityDeduped = try SharedSponsorLexiconPublisher.dedupe(entries: vanitySubdomainEntries)
        #expect(vanityDeduped.map(\.canonicalName) == ["BetterHelp", "Squarespace"])
    }

    @Test("controls and diagnostics expose default-on consumption and no private text")
    func controlsAndDiagnostics() throws {
        var settings = SharedSponsorLexiconSettings()
        #expect(settings.consumptionEnabled)
        #expect(!settings.contributionOptedIn)
        settings.contributionOptedIn = true
        settings.disableConsumption()
        #expect(!settings.consumptionEnabled)
        settings.resetToDefaults()
        #expect(settings == SharedSponsorLexiconSettings())

        let diagnostics = SharedSponsorLexiconDiagnostics(
            artifactVersion: "2026.05.12.1",
            validationStatus: .valid,
            cacheAgeSeconds: 3,
            mergeCounts: SharedSponsorLexiconMergeCounts(localActive: 1, sharedCandidate: 2, sharedSuppressedByLocalOverride: 0),
            contributionEligibilityCounts: SharedSponsorContributionEligibilityCounts(total: 4, eligible: 1, excludedInactive: 1, excludedUnderconfirmed: 0, excludedSharedOrQuarantined: 1, excludedConflicted: 0, excludedHighRollback: 1)
        )
        let data = try JSONEncoder().encode(diagnostics)
        let json = String(data: data, encoding: .utf8)!
        #expect(!json.lowercased().contains("transcript"))
        #expect(!json.lowercased().contains("audio"))
        #expect(!json.lowercased().contains("listening"))
        #expect(!json.contains("Squarespace"))
    }

    private func signedArtifact(
        version: String = "2026.05.12.1",
        entries: [SharedSponsorLexiconEntry]
    ) throws -> SharedSponsorLexiconArtifact {
        try SharedSponsorLexiconArtifact.signed(
            artifactVersion: version,
            createdAt: "2026-05-12",
            entries: entries,
            keyId: "phase12-test",
            signer: signer
        )
    }

    private func verifiedArtifact(
        _ artifact: SharedSponsorLexiconArtifact
    ) throws -> VerifiedSharedSponsorLexiconArtifact {
        try SharedSponsorLexiconValidator(verifier: signer).decodeAndValidate(artifact.encodedData())
    }

    private func entry(
        id: String = "sponsor",
        name: String,
        aliases: [String] = [],
        vanityURLs: [String] = [],
        ctaPatternDescriptors: [String] = [],
        disclosurePhrases: [String] = []
    ) -> SharedSponsorLexiconEntry {
        SharedSponsorLexiconEntry(
            id: id,
            canonicalName: name,
            aliases: aliases,
            vanityURLs: vanityURLs,
            ctaPatternDescriptors: ctaPatternDescriptors,
            disclosurePhrases: disclosurePhrases
        )
    }

    private func chunk(_ text: String) -> TranscriptChunk {
        TranscriptChunk(
            id: UUID().uuidString,
            analysisAssetId: "asset-shared-lexicon",
            segmentFingerprint: UUID().uuidString,
            chunkIndex: 0,
            startTime: 0,
            endTime: 10,
            text: text,
            normalizedText: TranscriptEngineService.normalizeText(text),
            pass: "final",
            modelVersion: "test",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
    }

    private func temporaryDirectory() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("SharedSponsorLexiconTests-\(UUID().uuidString)", isDirectory: true)
    }

    private func injectEntryField(_ key: String, value: Any, into data: Data) throws -> Data {
        var object = try JSONSerialization.jsonObject(with: data) as! [String: Any]
        var entries = object["entries"] as! [[String: Any]]
        entries[0][key] = value
        object["entries"] = entries
        return try JSONSerialization.data(withJSONObject: object, options: [.sortedKeys])
    }

    private func injectTopLevelField(_ key: String, value: String, into data: Data) throws -> Data {
        var object = try JSONSerialization.jsonObject(with: data) as! [String: Any]
        object[key] = value
        return try JSONSerialization.data(withJSONObject: object, options: [.sortedKeys])
    }

    private func injectNestedField(_ objectKey: String, key: String, value: String, into data: Data) throws -> Data {
        var object = try JSONSerialization.jsonObject(with: data) as! [String: Any]
        var nested = object[objectKey] as! [String: Any]
        nested[key] = value
        object[objectKey] = nested
        return try JSONSerialization.data(withJSONObject: object, options: [.sortedKeys])
    }

    private func removeTopLevelField(_ key: String, from data: Data) throws -> Data {
        var object = try JSONSerialization.jsonObject(with: data) as! [String: Any]
        object.removeValue(forKey: key)
        return try JSONSerialization.data(withJSONObject: object, options: [.sortedKeys])
    }

    private func forbiddenFieldSamples() -> [(String, String)] {
        [
            ("transcript", "transcript words"),
            ("audio", "audio bytes"),
            ("rawEvidence", "raw evidence snippet"),
            ("quote", "\"I use this every day\""),
            ("timestamp", "00:31"),
            ("episodeId", "episode-abc123"),
            ("assetId", "asset-def456"),
            ("userId", "user-ghi789"),
            ("accountId", "account-jkl012"),
            ("deviceId", "device-mno345"),
            ("profileId", "profile-pqr678"),
            ("subscription", "subscribed feed"),
            ("listeningHistory", "listening history record"),
        ]
    }

    private func forbiddenPublicTermSamples() -> [String] {
        [
            "raw evidence snippet",
            "transcript snippet from the host",
            "audio payload bytes",
            "listening history record",
            "550e8400-e29b-41d4-a716-446655440000",
            "account-jkl012",
            "device-mno345",
            "profile-pqr678",
            "account id jkl012",
            "device identifier mno345",
            "profile id pqr678",
            "listener id abc123",
            "customer identifier cst789",
            "account-abcdef",
            "device-mnopqr",
            "profile-pqrsuv",
            "listener-abcdef",
            "customer-cstxyz",
            "promo account-abcdef offer",
            "shared device-mnopqr descriptor",
            "public listener-abcdef token",
            "customer-cstxyz campaign",
            "listener_id_abcdef",
            "customer-identifier-cstxyz",
            "account id jklabc",
            "device identifier mnopqr",
            "profile id pqrsuv",
            "listener id abcdef",
            "customer identifier cstxyz",
            "episode-id-feedabc",
            "asset_identifier_feedabc",
            "user-id-abcdef",
            "asset identifier feedabc",
            "episodeid_feedabc",
            "assetidentifier_feedabc",
            "userid_abcdef",
            "useridabcdef",
            "accountid_jklabc",
            "deviceidentifier_mnopqr",
            "profileidpqrsuv",
            "listeneridentifierabcdef",
            "customeridcstxyz",
            "subscription-sub123",
            "subscription-abcdef",
            "subscription id subabc",
            "subscription identifier subabc",
            "subscription-identifier-subabc",
            "subscription_id_subabc",
            "subscriptionid_subabc",
            "subscribed-feed record",
            "listening-history record",
            "host said use code podcast",
            "this episode is brought to you by Squarespace right after the intro",
        ]
    }
}
