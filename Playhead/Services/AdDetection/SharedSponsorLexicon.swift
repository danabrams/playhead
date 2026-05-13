// SharedSponsorLexicon.swift
// Phase 12 (playhead-4my.12.3-playhead-4my.12.9): signed shared sponsor
// lexicon artifacts, device cache/load policy, privacy-safe contribution
// export, and deterministic local publish pipeline.

import CryptoKit
import Foundation

// MARK: - Schema

enum SharedSponsorLexiconSchema {
    static let supportedVersion = 1
    static let canonicalization = "playhead.shared-sponsor-lexicon.canonical-json.v1"
}

enum SharedSponsorLexiconError: Error, Equatable {
    case incompatibleSchema(Int)
    case malformedArtifact(String)
    case unknownField(String)
    case invalidSignature
    case privacyViolation(String)
    case invalidEntry(String)
    case disabled
}

enum SharedSponsorLexiconValidationStatus: String, Sendable, Codable, Equatable {
    case notLoaded
    case valid
    case disabled
    case downloadFailed
    case incompatibleSchema
    case invalidSignature
    case malformedArtifact
    case privacyViolation
    case corruptCache
}

enum SharedSponsorLexiconEntryType: String, Codable, Sendable, CaseIterable {
    case sponsor
    case cta
    case url
    case disclosure

    var knowledgeType: KnowledgeEntityType {
        switch self {
        case .sponsor: return .sponsor
        case .cta: return .cta
        case .url: return .url
        case .disclosure: return .disclosure
        }
    }
}

/// Public-only sponsor fact suitable for a shared lexicon artifact.
///
/// Allowed fields are intentionally narrow: canonical sponsor names, public
/// aliases, vanity URLs, CTA pattern descriptors, disclosure phrases, and
/// public type/category/provenance/version metadata. Transcript/audio/raw
/// evidence, quotes, timestamps, episode/asset/user identifiers, subscription
/// data, and listening history are rejected before decoding and before export.
struct SharedSponsorLexiconEntry: Codable, Sendable, Equatable {
    let id: String
    let canonicalName: String
    let aliases: [String]
    let vanityURLs: [String]
    let ctaPatternDescriptors: [String]
    let disclosurePhrases: [String]
    let type: SharedSponsorLexiconEntryType
    let category: String
    let provenance: String
    let version: String

    init(
        id: String,
        canonicalName: String,
        aliases: [String] = [],
        vanityURLs: [String] = [],
        ctaPatternDescriptors: [String] = [],
        disclosurePhrases: [String] = [],
        type: SharedSponsorLexiconEntryType = .sponsor,
        category: String = "sponsor",
        provenance: String = "local-confirmed",
        version: String = "1"
    ) {
        self.id = id
        self.canonicalName = canonicalName
        self.aliases = aliases
        self.vanityURLs = vanityURLs
        self.ctaPatternDescriptors = ctaPatternDescriptors
        self.disclosurePhrases = disclosurePhrases
        self.type = type
        self.category = category
        self.provenance = provenance
        self.version = version
    }

    var publicTerms: [String] {
        [canonicalName] + aliases + vanityURLs + ctaPatternDescriptors + disclosurePhrases
    }

    var normalizedIdentity: String {
        Self.normalize(canonicalName)
    }

    var compactIdentity: String {
        Self.compact(canonicalName)
    }

    func standardized() throws -> SharedSponsorLexiconEntry {
        let name = Self.collapseWhitespace(canonicalName)
        guard !name.isEmpty else { throw SharedSponsorLexiconError.invalidEntry("empty canonicalName") }
        guard !id.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw SharedSponsorLexiconError.invalidEntry("empty id")
        }
        guard !category.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw SharedSponsorLexiconError.invalidEntry("empty category")
        }
        guard !provenance.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw SharedSponsorLexiconError.invalidEntry("empty provenance")
        }
        guard !version.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw SharedSponsorLexiconError.invalidEntry("empty version")
        }

        return SharedSponsorLexiconEntry(
            id: Self.normalize(id),
            canonicalName: name,
            aliases: Self.cleanedSortedPublicTerms(aliases),
            vanityURLs: Self.cleanedSortedPublicTerms(vanityURLs.map { $0.lowercased() }),
            ctaPatternDescriptors: Self.cleanedSortedPublicTerms(ctaPatternDescriptors),
            disclosurePhrases: Self.cleanedSortedPublicTerms(disclosurePhrases),
            type: type,
            category: Self.normalize(category),
            provenance: Self.normalize(provenance),
            version: version.trimmingCharacters(in: .whitespacesAndNewlines)
        )
    }

    static func normalize(_ value: String) -> String {
        collapseWhitespace(value).lowercased()
    }

    static func compact(_ value: String) -> String {
        normalize(value).filter { $0.isLetter || $0.isNumber }
    }

    static func collapseWhitespace(_ value: String) -> String {
        value
            .split(whereSeparator: { $0.isWhitespace })
            .joined(separator: " ")
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private static func cleanedSortedPublicTerms(_ terms: [String]) -> [String] {
        let cleaned = terms
            .map { collapseWhitespace($0) }
            .filter { !$0.isEmpty }
        return Array(Set(cleaned)).sorted {
            normalize($0) == normalize($1) ? $0 < $1 : normalize($0) < normalize($1)
        }
    }
}

struct SharedSponsorLexiconManifest: Codable, Sendable, Equatable {
    let schemaVersion: Int
    let artifactVersion: String
    let keyId: String
    let createdAt: String
    let canonicalization: String
    let contentDigest: String
}

struct SharedSponsorLexiconSignature: Codable, Sendable, Equatable {
    let keyId: String
    let algorithm: String
    let value: String
}

private struct SharedSponsorLexiconUnsignedPayload: Codable, Sendable {
    let schemaVersion: Int
    let artifactVersion: String
    let manifest: SharedSponsorLexiconManifest
    let entries: [SharedSponsorLexiconEntry]
}

/// A signed, versioned shared sponsor lexicon artifact.
struct SharedSponsorLexiconArtifact: Codable, Sendable, Equatable {
    let schemaVersion: Int
    let artifactVersion: String
    let manifest: SharedSponsorLexiconManifest
    let entries: [SharedSponsorLexiconEntry]
    let signature: SharedSponsorLexiconSignature

    static func signed(
        artifactVersion: String,
        createdAt: String,
        entries: [SharedSponsorLexiconEntry],
        keyId: String,
        signer: SharedSponsorLexiconSigning
    ) throws -> SharedSponsorLexiconArtifact {
        let standardizedEntries = try entries
            .map { try $0.standardized() }
            .sorted(by: Self.entrySort)
        try SharedSponsorPrivacyGuard.validatePublicTerms(standardizedEntries.flatMap(\.publicTerms))

        let contentDigest = Self.digest(
            try Self.encodeCanonical(standardizedEntries)
        )
        let manifest = SharedSponsorLexiconManifest(
            schemaVersion: SharedSponsorLexiconSchema.supportedVersion,
            artifactVersion: artifactVersion,
            keyId: keyId,
            createdAt: createdAt,
            canonicalization: SharedSponsorLexiconSchema.canonicalization,
            contentDigest: contentDigest
        )
        let unsigned = SharedSponsorLexiconUnsignedPayload(
            schemaVersion: SharedSponsorLexiconSchema.supportedVersion,
            artifactVersion: artifactVersion,
            manifest: manifest,
            entries: standardizedEntries
        )
        let canonical = try Self.encodeCanonical(unsigned)
        try SharedSponsorPrivacyGuard.validateJSONData(canonical)
        let signature = try signer.sign(data: canonical, keyId: keyId)
        let artifact = SharedSponsorLexiconArtifact(
            schemaVersion: SharedSponsorLexiconSchema.supportedVersion,
            artifactVersion: artifactVersion,
            manifest: manifest,
            entries: standardizedEntries,
            signature: signature
        )
        try SharedSponsorPrivacyGuard.validateJSONData(artifact.encodedData())
        return artifact
    }

    func canonicalPayloadData() throws -> Data {
        let standardizedEntries = try entries
            .map { try $0.standardized() }
            .sorted(by: Self.entrySort)
        let unsigned = SharedSponsorLexiconUnsignedPayload(
            schemaVersion: schemaVersion,
            artifactVersion: artifactVersion,
            manifest: manifest,
            entries: standardizedEntries
        )
        return try Self.encodeCanonical(unsigned)
    }

    func encodedData() throws -> Data {
        try Self.encodeCanonical(self)
    }

    func quarantineEntries(podcastId: String) -> [SponsorKnowledgeEntry] {
        entries.map { entry in
            SponsorKnowledgeEntry(
                id: "shared:\(artifactVersion):\(entry.id)",
                podcastId: podcastId,
                entityType: entry.type.knowledgeType,
                entityValue: entry.canonicalName,
                normalizedValue: entry.normalizedIdentity,
                state: .quarantined,
                confirmationCount: 0,
                rollbackCount: 0,
                aliases: entry.aliases,
                metadata: [
                    "origin": "shared",
                    "sharedArtifactVersion": artifactVersion,
                    "sharedEntryId": entry.id,
                ]
            )
        }
    }

    static func entrySort(_ lhs: SharedSponsorLexiconEntry, _ rhs: SharedSponsorLexiconEntry) -> Bool {
        if lhs.normalizedIdentity != rhs.normalizedIdentity {
            return lhs.normalizedIdentity < rhs.normalizedIdentity
        }
        return lhs.id < rhs.id
    }

    static func encodeCanonical<T: Encodable>(_ value: T) throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
        return try encoder.encode(value)
    }

    static func digest(_ data: Data) -> String {
        "sha256:" + SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    }
}

/// Validator-issued wrapper for a shared artifact that has passed schema,
/// privacy, digest, and signature checks. Merge/cache call sites take this
/// type so raw decoded artifacts cannot bypass mandatory verification.
struct VerifiedSharedSponsorLexiconArtifact: Sendable, Equatable {
    let artifact: SharedSponsorLexiconArtifact

    var artifactVersion: String { artifact.artifactVersion }
    var entries: [SharedSponsorLexiconEntry] { artifact.entries }
    var manifest: SharedSponsorLexiconManifest { artifact.manifest }
    var signature: SharedSponsorLexiconSignature { artifact.signature }

    fileprivate init(_ artifact: SharedSponsorLexiconArtifact) {
        self.artifact = artifact
    }

    func encodedData() throws -> Data {
        try artifact.encodedData()
    }

    func quarantineEntries(podcastId: String) -> [SponsorKnowledgeEntry] {
        artifact.quarantineEntries(podcastId: podcastId)
    }
}

// MARK: - Signing

protocol SharedSponsorLexiconSigning: Sendable {
    func sign(data: Data, keyId: String) throws -> SharedSponsorLexiconSignature
}

protocol SharedSponsorLexiconVerifying: Sendable {
    func verify(signature: SharedSponsorLexiconSignature, data: Data) throws -> Bool
}

/// Deterministic test signer/verifier. This is not production key management;
/// it gives tests a mandatory verification boundary with fail-closed behavior.
struct DeterministicSharedSponsorLexiconSigner: SharedSponsorLexiconSigning, SharedSponsorLexiconVerifying {
    let secret: String
    let algorithm = "test-sha256"

    func sign(data: Data, keyId: String) throws -> SharedSponsorLexiconSignature {
        var material = Data(secret.utf8)
        material.append(Data("\n".utf8))
        material.append(data)
        let value = SHA256.hash(data: material).map { String(format: "%02x", $0) }.joined()
        return SharedSponsorLexiconSignature(keyId: keyId, algorithm: algorithm, value: value)
    }

    func verify(signature: SharedSponsorLexiconSignature, data: Data) throws -> Bool {
        guard signature.algorithm == algorithm else { return false }
        return try sign(data: data, keyId: signature.keyId).value == signature.value
    }
}

struct SharedSponsorLexiconValidator: Sendable {
    let verifier: SharedSponsorLexiconVerifying

    func decodeAndValidate(_ data: Data) throws -> VerifiedSharedSponsorLexiconArtifact {
        do {
            try SharedSponsorPrivacyGuard.validateJSONData(data)
            try SharedSponsorLexiconJSONShape.validateArtifact(data)

            let decoder = JSONDecoder()
            let artifact = try decoder.decode(SharedSponsorLexiconArtifact.self, from: data)
            return try validate(artifact)
        } catch let error as SharedSponsorLexiconError {
            throw error
        } catch {
            throw SharedSponsorLexiconError.malformedArtifact("invalid artifact JSON")
        }
    }

    func validate(_ artifact: SharedSponsorLexiconArtifact) throws -> VerifiedSharedSponsorLexiconArtifact {
        try SharedSponsorPrivacyGuard.validateJSONData(artifact.encodedData())
        guard artifact.schemaVersion == SharedSponsorLexiconSchema.supportedVersion else {
            throw SharedSponsorLexiconError.incompatibleSchema(artifact.schemaVersion)
        }
        guard artifact.manifest.schemaVersion == artifact.schemaVersion else {
            throw SharedSponsorLexiconError.malformedArtifact("manifest schema mismatch")
        }
        guard artifact.manifest.artifactVersion == artifact.artifactVersion else {
            throw SharedSponsorLexiconError.malformedArtifact("manifest version mismatch")
        }
        guard artifact.manifest.canonicalization == SharedSponsorLexiconSchema.canonicalization else {
            throw SharedSponsorLexiconError.malformedArtifact("unknown canonicalization")
        }
        guard artifact.manifest.keyId == artifact.signature.keyId else {
            throw SharedSponsorLexiconError.invalidSignature
        }

        let entries = try artifact.entries
            .map { try $0.standardized() }
            .sorted(by: SharedSponsorLexiconArtifact.entrySort)
        guard artifact.entries == entries else {
            throw SharedSponsorLexiconError.malformedArtifact("non-canonical entries")
        }
        try SharedSponsorPrivacyGuard.validatePublicTerms(entries.flatMap(\.publicTerms))
        let digest = SharedSponsorLexiconArtifact.digest(
            try SharedSponsorLexiconArtifact.encodeCanonical(entries)
        )
        guard digest == artifact.manifest.contentDigest else {
            throw SharedSponsorLexiconError.malformedArtifact("content digest mismatch")
        }
        let canonicalPayload = try artifact.canonicalPayloadData()
        try SharedSponsorPrivacyGuard.validateJSONData(canonicalPayload)
        guard try verifier.verify(signature: artifact.signature, data: canonicalPayload) else {
            throw SharedSponsorLexiconError.invalidSignature
        }
        return VerifiedSharedSponsorLexiconArtifact(artifact)
    }
}

// MARK: - Privacy and JSON shape validation

enum SharedSponsorPrivacyGuard {
    private static let forbiddenKeyFragments = [
        "transcript", "audio", "rawevidence", "raw_evidence", "evidence",
        "quote", "timestamp", "episodeid", "episode_id", "assetid",
        "asset_id", "userid", "user_id", "accountid", "account_id",
        "deviceid", "device_id", "profileid", "profile_id", "listenerid",
        "listener_id", "customerid", "customer_id", "subscription",
        "listening", "history",
    ]
    private static let forbiddenStringFragments = [
        "raw evidence", "transcript snippet", "transcript text",
        "audio payload", "audio bytes", "listening history",
        "subscribed feed", "episode id", "asset id", "user id",
    ]
    private static let contentTimestampPattern = #"\b\d{1,2}:\d{2}(:\d{2})?\b"#
    private static let idPattern = #"\b(episode|asset|user)[-_][A-Za-z0-9]{3,}\b"#
    private static let privateIdentifierPattern = #"\b(account|device|profile|listener|customer|subscription)[-_](?=[A-Za-z0-9]*\d)[A-Za-z0-9]{3,}\b"#
    private static let privateAllLetterIdentifierPattern = #"\b(account|device|profile|listener|customer|subscription)[-_]([A-Za-z]{3,})\b"#
    private static let safeAllLetterPrivateIdentifierSuffixes: Set<String> = [
        "based", "supported",
    ]
    private static let privateIdentifierTokenPattern = #"\b(account|device|profile|listener|customer|subscription)[-_](?:id|identifier)(?:\s*[:#-]\s*|[_-])[A-Za-z0-9_-]{3,}\b"#
    private static let privateIdentifierPhrasePattern = #"\b(account|device|profile|listener|customer|subscription)\s+(?:id|identifier)\s*[:#-]?\s*[A-Za-z0-9_-]{3,}\b"#
    private static let episodeIdentifierTokenPattern = #"\b(episode|asset|user)[-_](?:id|identifier)(?:\s*[:#-]\s*|[_-])[A-Za-z0-9_-]{3,}\b"#
    private static let episodeIdentifierPhrasePattern = #"\b(episode|asset|user)\s+(?:id|identifier)\s*[:#-]?\s*[A-Za-z0-9_-]{3,}\b"#
    private static let compactIdentifierTokenPattern = #"\b(episode|asset|user|account|device|profile|listener|customer|subscription)(?:id|identifier)(?:\s*[:#-]\s*|[_-])[A-Za-z0-9_-]{3,}\b"#
    private static let compactIdentifierSuffixPattern = #"\b(episode|asset|user|account|device|profile|listener|customer|subscription)(?:id|identifier)[A-Za-z0-9]{3,}\b"#
    private static let uuidPattern = #"\b[0-9A-F]{8}-[0-9A-F]{4}-[1-5][0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}\b"#
    private static let emailPattern = #"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}"#

    static func validateJSONData(_ data: Data) throws {
        let object = try JSONSerialization.jsonObject(with: data)
        try validateJSONObject(object, keyPath: [])
    }

    static func validatePublicTerms(_ terms: [String]) throws {
        for term in terms {
            try validatePublicString(term, keyPath: ["publicTerm"])
        }
    }

    private static func validateJSONObject(_ object: Any, keyPath: [String]) throws {
        if let dict = object as? [String: Any] {
            for (key, value) in dict {
                let normalizedKey = key.replacingOccurrences(of: "-", with: "_").lowercased()
                if forbiddenKeyFragments.contains(where: { normalizedKey.contains($0) }) {
                    throw SharedSponsorLexiconError.privacyViolation((keyPath + [key]).joined(separator: "."))
                }
                try validateJSONObject(value, keyPath: keyPath + [key])
            }
        } else if let array = object as? [Any] {
            for (index, value) in array.enumerated() {
                try validateJSONObject(value, keyPath: keyPath + ["[\(index)]"])
            }
        } else if let string = object as? String {
            if !isKnownSafeMetadataString(string, keyPath: keyPath) {
                try validatePublicString(string, keyPath: keyPath)
            }
        }
    }

    private static func validatePublicString(_ string: String, keyPath: [String]) throws {
        let range = NSRange(string.startIndex..<string.endIndex, in: string)
        if string.contains("\"") || string.contains("“") || string.contains("”") {
            throw SharedSponsorLexiconError.privacyViolation(keyPath.joined(separator: "."))
        }
        if regex(contentTimestampPattern).firstMatch(in: string, range: range) != nil {
            throw SharedSponsorLexiconError.privacyViolation(keyPath.joined(separator: "."))
        }
        if regex(idPattern, options: [.caseInsensitive]).firstMatch(in: string, range: range) != nil {
            throw SharedSponsorLexiconError.privacyViolation(keyPath.joined(separator: "."))
        }
        if regex(privateIdentifierPattern, options: [.caseInsensitive]).firstMatch(in: string, range: range) != nil {
            throw SharedSponsorLexiconError.privacyViolation(keyPath.joined(separator: "."))
        }
        if containsPrivateAllLetterIdentifier(string) {
            throw SharedSponsorLexiconError.privacyViolation(keyPath.joined(separator: "."))
        }
        if regex(privateIdentifierTokenPattern, options: [.caseInsensitive]).firstMatch(in: string, range: range) != nil {
            throw SharedSponsorLexiconError.privacyViolation(keyPath.joined(separator: "."))
        }
        if regex(privateIdentifierPhrasePattern, options: [.caseInsensitive]).firstMatch(in: string, range: range) != nil {
            throw SharedSponsorLexiconError.privacyViolation(keyPath.joined(separator: "."))
        }
        if regex(episodeIdentifierTokenPattern, options: [.caseInsensitive]).firstMatch(in: string, range: range) != nil {
            throw SharedSponsorLexiconError.privacyViolation(keyPath.joined(separator: "."))
        }
        if regex(episodeIdentifierPhrasePattern, options: [.caseInsensitive]).firstMatch(in: string, range: range) != nil {
            throw SharedSponsorLexiconError.privacyViolation(keyPath.joined(separator: "."))
        }
        if regex(compactIdentifierTokenPattern, options: [.caseInsensitive]).firstMatch(in: string, range: range) != nil {
            throw SharedSponsorLexiconError.privacyViolation(keyPath.joined(separator: "."))
        }
        if regex(compactIdentifierSuffixPattern, options: [.caseInsensitive]).firstMatch(in: string, range: range) != nil {
            throw SharedSponsorLexiconError.privacyViolation(keyPath.joined(separator: "."))
        }
        if regex(uuidPattern, options: [.caseInsensitive]).firstMatch(in: string, range: range) != nil {
            throw SharedSponsorLexiconError.privacyViolation(keyPath.joined(separator: "."))
        }
        if regex(emailPattern, options: [.caseInsensitive]).firstMatch(in: string, range: range) != nil {
            throw SharedSponsorLexiconError.privacyViolation(keyPath.joined(separator: "."))
        }
        let lowered = string.lowercased()
        let separatorNormalized = lowered.replacingOccurrences(
            of: #"[-_]+"#,
            with: " ",
            options: .regularExpression
        )
        if forbiddenStringFragments.contains(where: lowered.contains)
            || forbiddenStringFragments.contains(where: separatorNormalized.contains)
            || isTranscriptLikeSnippet(lowered) {
            throw SharedSponsorLexiconError.privacyViolation(keyPath.joined(separator: "."))
        }
    }

    private static func isTranscriptLikeSnippet(_ lowered: String) -> Bool {
        if lowered.contains("this episode")
            || lowered.contains("today s episode")
            || lowered.contains("today's episode")
            || lowered.contains("host said")
            || lowered.contains("right after the intro")
            || lowered.contains("after the intro") {
            return true
        }

        let words = lowered.split { !$0.isLetter && !$0.isNumber }
        guard words.count >= 8 else { return false }
        return lowered.contains("sponsor")
            || lowered.contains("brought to you")
            || lowered.contains("use code")
    }

    private static func isKnownSafeMetadataString(_ string: String, keyPath: [String]) -> Bool {
        switch keyPath {
        case ["manifest", "createdAt"]:
            return matches(#"\A\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}Z)?\z"#, string)
        case ["manifest", "contentDigest"]:
            return matches(#"\Asha256:[0-9a-f]{64}\z"#, string)
        case ["signature", "value"]:
            return matches(#"\A[0-9a-f]{64}\z"#, string)
        default:
            return false
        }
    }

    private static func containsPrivateAllLetterIdentifier(_ string: String) -> Bool {
        let regex = regex(privateAllLetterIdentifierPattern, options: [.caseInsensitive])
        let range = NSRange(string.startIndex..<string.endIndex, in: string)
        let matches = regex.matches(in: string, range: range)
        for match in matches {
            guard match.numberOfRanges > 2,
                  let suffixRange = Range(match.range(at: 2), in: string)
            else {
                return true
            }
            let suffix = String(string[suffixRange]).lowercased()
            if !safeAllLetterPrivateIdentifierSuffixes.contains(suffix) {
                return true
            }
        }
        return false
    }

    private static func matches(_ pattern: String, _ string: String) -> Bool {
        let range = NSRange(string.startIndex..<string.endIndex, in: string)
        return regex(pattern, options: [.caseInsensitive]).firstMatch(in: string, range: range) != nil
    }

    private static func regex(
        _ pattern: String,
        options: NSRegularExpression.Options = []
    ) -> NSRegularExpression {
        // Patterns above are constants validated by unit tests; invalid here is a programmer error.
        try! NSRegularExpression(pattern: pattern, options: options)
    }
}

enum SharedSponsorLexiconJSONShape {
    private static let artifactKeys: Set<String> = [
        "schemaVersion", "artifactVersion", "manifest", "entries", "signature",
    ]
    private static let manifestKeys: Set<String> = [
        "schemaVersion", "artifactVersion", "keyId", "createdAt", "canonicalization", "contentDigest",
    ]
    private static let signatureKeys: Set<String> = ["keyId", "algorithm", "value"]
    private static let entryKeys: Set<String> = [
        "id", "canonicalName", "aliases", "vanityURLs", "ctaPatternDescriptors",
        "disclosurePhrases", "type", "category", "provenance", "version",
    ]
    private static let contributionKeys: Set<String> = ["schemaVersion", "exporterVersion", "entries"]

    static func validateArtifact(_ data: Data) throws {
        let object = try JSONSerialization.jsonObject(with: data)
        guard let dict = object as? [String: Any] else {
            throw SharedSponsorLexiconError.malformedArtifact("top-level JSON is not an object")
        }
        try requireKeys(Set(dict.keys), allowed: artifactKeys, path: "artifact")
        if let manifest = dict["manifest"] as? [String: Any] {
            try requireKeys(Set(manifest.keys), allowed: manifestKeys, path: "manifest")
        }
        if let signature = dict["signature"] as? [String: Any] {
            try requireKeys(Set(signature.keys), allowed: signatureKeys, path: "signature")
        }
        guard let entries = dict["entries"] as? [[String: Any]] else {
            throw SharedSponsorLexiconError.malformedArtifact("entries is not an array")
        }
        for (index, entry) in entries.enumerated() {
            try requireKeys(Set(entry.keys), allowed: entryKeys, path: "entries[\(index)]")
        }
    }

    static func validateContribution(_ data: Data) throws {
        let object = try JSONSerialization.jsonObject(with: data)
        guard let dict = object as? [String: Any] else {
            throw SharedSponsorLexiconError.malformedArtifact("top-level contribution JSON is not an object")
        }
        try requireKeys(Set(dict.keys), allowed: contributionKeys, path: "contribution")
        guard let entries = dict["entries"] as? [[String: Any]] else {
            throw SharedSponsorLexiconError.malformedArtifact("entries is not an array")
        }
        for (index, entry) in entries.enumerated() {
            try requireKeys(Set(entry.keys), allowed: entryKeys, path: "entries[\(index)]")
        }
    }

    private static func requireKeys(_ actual: Set<String>, allowed: Set<String>, path: String) throws {
        if let unknown = actual.subtracting(allowed).sorted().first {
            throw SharedSponsorLexiconError.unknownField("\(path).\(unknown)")
        }
    }
}

enum SharedSponsorLexiconTermVariants {
    private static let ignoredHostLabels: Set<String> = [
        "ad", "ads", "app", "au", "ca", "co", "com", "de", "deal", "deals",
        "fm", "fr", "get", "go", "io", "join", "link", "links", "m", "net",
        "offer", "offers", "org", "podcast", "podcasts", "promo", "promos",
        "save", "shop", "try", "tv", "uk", "us", "visit", "www",
    ]

    static func comparableTerms(_ terms: [String]) -> Set<String> {
        Set(terms.flatMap(comparableTermVariants))
    }

    static func identityComparableTerms(
        canonicalName: String,
        aliases: [String],
        vanityURLs: [String]
    ) -> Set<String> {
        let identityAliases = aliases.filter { !isGenericDescriptor($0) }
        return comparableTerms([canonicalName] + identityAliases + vanityURLs)
    }

    static func comparableTermVariants(_ term: String) -> [String] {
        var variants = [
            SharedSponsorLexiconEntry.normalize(term),
            SharedSponsorLexiconEntry.compact(term),
        ]
        variants.append(contentsOf: urlHostStemVariants(term))
        return variants.filter { !$0.isEmpty }
    }

    static func isGenericDescriptor(_ term: String) -> Bool {
        let normalized = SharedSponsorLexiconEntry.normalize(term)
        guard !normalized.isEmpty else { return false }

        let genericPrefixes = [
            "use code ",
            "enter code ",
            "promo code ",
            "discount code ",
            "coupon code ",
        ]
        if genericPrefixes.contains(where: normalized.hasPrefix) {
            return true
        }

        let genericFragments = [
            " at checkout",
            "brought to you by",
            "sponsored by",
            "thanks to our sponsor",
            "today s sponsor",
            "today's sponsor",
            "this episode is sponsored",
            "this podcast is brought",
            "a word from our sponsor",
            "message from our sponsor",
            "supported by",
        ]
        return genericFragments.contains(where: normalized.contains)
    }

    private static func urlHostStemVariants(_ term: String) -> [String] {
        let normalized = SharedSponsorLexiconEntry.normalize(term)
            .replacingOccurrences(of: " dot ", with: ".")
            .replacingOccurrences(of: " slash ", with: "/")
        let withoutScheme = normalized
            .replacingOccurrences(of: #"^[a-z][a-z0-9+.-]*://"#, with: "", options: .regularExpression)
        let host = withoutScheme
            .split { $0 == "/" || $0.isWhitespace }
            .first
            .map(String.init) ?? ""
        guard host.contains(".") else { return [] }

        let labels = host
            .split(separator: ".")
            .map(String.init)
            .filter { !$0.isEmpty }
        guard labels.count >= 2 else { return [] }

        let candidates = labels.dropLast().filter { !ignoredHostLabels.contains($0) }
        return candidates.flatMap { label in
            [SharedSponsorLexiconEntry.normalize(label), SharedSponsorLexiconEntry.compact(label)]
        }
    }
}

// MARK: - Controls and diagnostics

struct SharedSponsorLexiconSettings: Sendable, Equatable {
    var consumptionEnabled: Bool = true
    var contributionOptedIn: Bool = false

    mutating func disableConsumption() {
        consumptionEnabled = false
    }

    mutating func resetToDefaults() {
        consumptionEnabled = true
        contributionOptedIn = false
    }
}

struct SharedSponsorLexiconMergeCounts: Sendable, Equatable, Codable {
    var localActive: Int = 0
    var sharedCandidate: Int = 0
    var sharedSuppressedByLocalOverride: Int = 0
}

struct SharedSponsorContributionEligibilityCounts: Sendable, Equatable, Codable {
    var total: Int = 0
    var eligible: Int = 0
    var excludedInactive: Int = 0
    var excludedUnderconfirmed: Int = 0
    var excludedSharedOrQuarantined: Int = 0
    var excludedConflicted: Int = 0
    var excludedHighRollback: Int = 0
}

struct SharedSponsorLexiconDiagnostics: Sendable, Equatable, Codable {
    var artifactVersion: String?
    var validationStatus: SharedSponsorLexiconValidationStatus
    var cacheAgeSeconds: Double?
    var mergeCounts: SharedSponsorLexiconMergeCounts
    var contributionEligibilityCounts: SharedSponsorContributionEligibilityCounts
}

// MARK: - Device cache client

protocol SharedSponsorLexiconTransport: Sendable {
    func fetchArtifactData() async throws -> Data
}

struct ClosureSharedSponsorLexiconTransport: SharedSponsorLexiconTransport {
    let fetch: @Sendable () async throws -> Data

    func fetchArtifactData() async throws -> Data {
        try await fetch()
    }
}

actor SharedSponsorLexiconClient {
    private let transport: SharedSponsorLexiconTransport
    private let validator: SharedSponsorLexiconValidator
    private let cacheDirectory: URL
    private let fileManager: FileManager
    private var lastKnownGood: VerifiedSharedSponsorLexiconArtifact?
    private var settings: SharedSponsorLexiconSettings
    private(set) var diagnostics: SharedSponsorLexiconDiagnostics

    private var cacheURL: URL { cacheDirectory.appendingPathComponent("shared-sponsor-lexicon-lkg.json") }

    init(
        transport: SharedSponsorLexiconTransport,
        validator: SharedSponsorLexiconValidator,
        cacheDirectory: URL,
        settings: SharedSponsorLexiconSettings = SharedSponsorLexiconSettings(),
        fileManager: FileManager = .default
    ) {
        self.transport = transport
        self.validator = validator
        self.cacheDirectory = cacheDirectory
        self.settings = settings
        self.fileManager = fileManager
        self.diagnostics = SharedSponsorLexiconDiagnostics(
            artifactVersion: nil,
            validationStatus: settings.consumptionEnabled ? .notLoaded : .disabled,
            cacheAgeSeconds: nil,
            mergeCounts: SharedSponsorLexiconMergeCounts(),
            contributionEligibilityCounts: SharedSponsorContributionEligibilityCounts()
        )
    }

    func updateSettings(_ settings: SharedSponsorLexiconSettings) {
        self.settings = settings
        if !settings.consumptionEnabled {
            diagnostics.validationStatus = .disabled
        }
    }

    func loadFromCache() -> VerifiedSharedSponsorLexiconArtifact? {
        guard settings.consumptionEnabled else {
            diagnostics.validationStatus = .disabled
            return nil
        }
        guard fileManager.fileExists(atPath: cacheURL.path) else {
            diagnostics.validationStatus = .notLoaded
            diagnostics.cacheAgeSeconds = nil
            return lastKnownGood
        }
        do {
            let data = try Data(contentsOf: cacheURL)
            let artifact = try validator.decodeAndValidate(data)
            lastKnownGood = artifact
            updateDiagnostics(status: .valid, artifact: artifact)
            return artifact
        } catch SharedSponsorLexiconError.invalidSignature {
            return preserveLastKnownGood(status: .invalidSignature)
        } catch SharedSponsorLexiconError.privacyViolation {
            return preserveLastKnownGood(status: .privacyViolation)
        } catch SharedSponsorLexiconError.incompatibleSchema {
            return preserveLastKnownGood(status: .incompatibleSchema)
        } catch SharedSponsorLexiconError.malformedArtifact,
                SharedSponsorLexiconError.unknownField,
                SharedSponsorLexiconError.invalidEntry {
            return preserveLastKnownGood(status: .malformedArtifact)
        } catch {
            return preserveLastKnownGood(status: .corruptCache)
        }
    }

    func refresh() async -> VerifiedSharedSponsorLexiconArtifact? {
        guard settings.consumptionEnabled else {
            diagnostics.validationStatus = .disabled
            return nil
        }
        do {
            let data = try await transport.fetchArtifactData()
            let artifact = try validator.decodeAndValidate(data)
            try promoteToCache(data)
            lastKnownGood = artifact
            updateDiagnostics(status: .valid, artifact: artifact)
            return artifact
        } catch SharedSponsorLexiconError.invalidSignature {
            return preserveLastKnownGood(status: .invalidSignature)
        } catch SharedSponsorLexiconError.privacyViolation {
            return preserveLastKnownGood(status: .privacyViolation)
        } catch SharedSponsorLexiconError.incompatibleSchema {
            return preserveLastKnownGood(status: .incompatibleSchema)
        } catch SharedSponsorLexiconError.malformedArtifact,
                SharedSponsorLexiconError.unknownField,
                SharedSponsorLexiconError.invalidEntry {
            return preserveLastKnownGood(status: .malformedArtifact)
        } catch {
            return preserveLastKnownGood(status: .downloadFailed)
        }
    }

    private func promoteToCache(_ data: Data) throws {
        try fileManager.createDirectory(at: cacheDirectory, withIntermediateDirectories: true)
        let temp = cacheDirectory.appendingPathComponent("shared-sponsor-lexicon-lkg.tmp")
        try data.write(to: temp, options: [.atomic])
        if fileManager.fileExists(atPath: cacheURL.path) {
            _ = try fileManager.replaceItemAt(cacheURL, withItemAt: temp, backupItemName: nil)
        } else {
            try fileManager.moveItem(at: temp, to: cacheURL)
        }
    }

    private func updateDiagnostics(
        status: SharedSponsorLexiconValidationStatus,
        artifact: VerifiedSharedSponsorLexiconArtifact
    ) {
        diagnostics.validationStatus = status
        diagnostics.artifactVersion = artifact.artifactVersion
        updateCacheAge()
    }

    private func preserveLastKnownGood(
        status: SharedSponsorLexiconValidationStatus
    ) -> VerifiedSharedSponsorLexiconArtifact? {
        if lastKnownGood == nil, let cached = loadCachedLastKnownGood() {
            lastKnownGood = cached
        }
        diagnostics.validationStatus = status
        diagnostics.artifactVersion = lastKnownGood?.artifactVersion
        updateCacheAge()
        return lastKnownGood
    }

    private func loadCachedLastKnownGood() -> VerifiedSharedSponsorLexiconArtifact? {
        guard fileManager.fileExists(atPath: cacheURL.path),
              let data = try? Data(contentsOf: cacheURL)
        else {
            return nil
        }
        return try? validator.decodeAndValidate(data)
    }

    private func updateCacheAge() {
        guard let attributes = try? fileManager.attributesOfItem(atPath: cacheURL.path),
              let modified = attributes[.modificationDate] as? Date
        else {
            diagnostics.cacheAgeSeconds = nil
            return
        }
        diagnostics.cacheAgeSeconds = max(0, Date().timeIntervalSince(modified))
    }
}

// MARK: - Contribution export

struct SharedSponsorContributionPayload: Codable, Sendable, Equatable {
    let schemaVersion: Int
    let exporterVersion: String
    let entries: [SharedSponsorLexiconEntry]

    fileprivate func standardizedEntries() throws -> [SharedSponsorLexiconEntry] {
        return try entries
            .map { try $0.standardized() }
            .sorted(by: SharedSponsorLexiconArtifact.entrySort)
    }

    func encodedData() throws -> Data {
        guard schemaVersion == SharedSponsorLexiconSchema.supportedVersion else {
            throw SharedSponsorLexiconError.incompatibleSchema(schemaVersion)
        }
        let canonical = SharedSponsorContributionPayload(
            schemaVersion: schemaVersion,
            exporterVersion: exporterVersion,
            entries: try standardizedEntries()
        )
        let data = try SharedSponsorLexiconArtifact.encodeCanonical(canonical)
        try SharedSponsorPrivacyGuard.validateJSONData(data)
        try SharedSponsorLexiconJSONShape.validateContribution(data)
        return data
    }
}

struct SharedSponsorContributionExporter: Sendable {
    let settings: SharedSponsorLexiconSettings
    let exporterVersion: String

    init(settings: SharedSponsorLexiconSettings, exporterVersion: String = "phase12-exporter-v1") {
        self.settings = settings
        self.exporterVersion = exporterVersion
    }

    func export(entries: [SponsorKnowledgeEntry]) throws -> SharedSponsorContributionPayload? {
        guard settings.contributionOptedIn else { return nil }
        let eligible = entries.filter(Self.isEligibleForContribution)
        let sharedEntries = try eligible.map(Self.publicEntry(from:)).map { try $0.standardized() }
        let payload = SharedSponsorContributionPayload(
            schemaVersion: SharedSponsorLexiconSchema.supportedVersion,
            exporterVersion: exporterVersion,
            entries: sharedEntries.sorted(by: SharedSponsorLexiconArtifact.entrySort)
        )
        _ = try payload.encodedData()
        return payload
    }

    func eligibilityCounts(entries: [SponsorKnowledgeEntry]) -> SharedSponsorContributionEligibilityCounts {
        var counts = SharedSponsorContributionEligibilityCounts(total: entries.count)
        for entry in entries {
            if Self.isSharedOrQuarantined(entry) {
                counts.excludedSharedOrQuarantined += 1
            } else if Self.isConflictedOrCorrected(entry) {
                counts.excludedConflicted += 1
            } else if entry.state != .active {
                counts.excludedInactive += 1
            } else if entry.rollbackRate > KnowledgePromotionThresholds.maxRollbackRateForActive {
                counts.excludedHighRollback += 1
            } else if entry.confirmationCount < KnowledgePromotionThresholds.minConfirmationsForActive {
                counts.excludedUnderconfirmed += 1
            } else {
                counts.eligible += 1
            }
        }
        return counts
    }

    static func isEligibleForContribution(_ entry: SponsorKnowledgeEntry) -> Bool {
        entry.state == .active
            && entry.confirmationCount >= KnowledgePromotionThresholds.minConfirmationsForActive
            && entry.rollbackRate <= KnowledgePromotionThresholds.maxRollbackRateForActive
            && !isSharedOrQuarantined(entry)
            && !isConflictedOrCorrected(entry)
    }

    private static func isSharedOrQuarantined(_ entry: SponsorKnowledgeEntry) -> Bool {
        entry.state == .quarantined || entry.metadata?["origin"] == "shared"
    }

    private static func isConflictedOrCorrected(_ entry: SponsorKnowledgeEntry) -> Bool {
        entry.metadata?["conflict"] == "true"
            || entry.metadata?["corrected"] == "true"
            || entry.metadata?["localOverride"] == "corrected"
    }

    private static func publicEntry(from entry: SponsorKnowledgeEntry) throws -> SharedSponsorLexiconEntry {
        let sharedType = SharedSponsorLexiconEntryType(rawValue: entry.entityType.rawValue) ?? .sponsor
        let aliases: [String]
        let vanityURLs: [String]
        let ctaPatternDescriptors: [String]
        let disclosurePhrases: [String]

        switch sharedType {
        case .sponsor:
            aliases = entry.aliases
            vanityURLs = []
            ctaPatternDescriptors = []
            disclosurePhrases = []
        case .url:
            aliases = []
            vanityURLs = [entry.entityValue] + entry.aliases
            ctaPatternDescriptors = []
            disclosurePhrases = []
        case .cta:
            aliases = []
            vanityURLs = []
            ctaPatternDescriptors = [entry.entityValue] + entry.aliases
            disclosurePhrases = []
        case .disclosure:
            aliases = []
            vanityURLs = []
            ctaPatternDescriptors = []
            disclosurePhrases = [entry.entityValue] + entry.aliases
        }

        let shared = SharedSponsorLexiconEntry(
            id: "\(entry.entityType.rawValue):\(entry.normalizedValue)",
            canonicalName: entry.entityValue,
            aliases: aliases,
            vanityURLs: vanityURLs,
            ctaPatternDescriptors: ctaPatternDescriptors,
            disclosurePhrases: disclosurePhrases,
            type: sharedType,
            category: entry.entityType.rawValue,
            provenance: "local-confirmed",
            version: "1"
        )
        try SharedSponsorPrivacyGuard.validatePublicTerms(shared.publicTerms)
        return shared
    }
}

// MARK: - Central publish pipeline

struct SharedSponsorLexiconPublishResult: Sendable, Equatable {
    let artifact: SharedSponsorLexiconArtifact
    let previousArtifact: SharedSponsorLexiconArtifact?
    let publishedEntryCount: Int
}

struct SharedSponsorLexiconPublisher: Sendable {
    private(set) var latestArtifact: SharedSponsorLexiconArtifact?
    private(set) var previousArtifact: SharedSponsorLexiconArtifact?

    mutating func publish(
        contributions: [SharedSponsorContributionPayload],
        artifactVersion: String,
        createdAt: String,
        keyId: String,
        signer: SharedSponsorLexiconSigning,
        blocklist: Set<String> = []
    ) throws -> SharedSponsorLexiconPublishResult {
        let validated = try contributions.flatMap { contribution -> [SharedSponsorLexiconEntry] in
            guard contribution.schemaVersion == SharedSponsorLexiconSchema.supportedVersion else {
                throw SharedSponsorLexiconError.incompatibleSchema(contribution.schemaVersion)
            }
            _ = try contribution.encodedData()
            return try contribution.standardizedEntries()
        }

        let entries = try Self.dedupe(entries: validated, blocklist: blocklist)
        let artifact = try SharedSponsorLexiconArtifact.signed(
            artifactVersion: artifactVersion,
            createdAt: createdAt,
            entries: entries,
            keyId: keyId,
            signer: signer
        )
        guard let verifier = signer as? any SharedSponsorLexiconVerifying else {
            throw SharedSponsorLexiconError.invalidSignature
        }
        _ = try SharedSponsorLexiconValidator(verifier: verifier).validate(artifact)
        previousArtifact = latestArtifact
        latestArtifact = artifact
        return SharedSponsorLexiconPublishResult(
            artifact: artifact,
            previousArtifact: previousArtifact,
            publishedEntryCount: artifact.entries.count
        )
    }

    static func dedupe(
        entries: [SharedSponsorLexiconEntry],
        blocklist: Set<String> = []
    ) throws -> [SharedSponsorLexiconEntry] {
        let normalizedBlocklist = Set(blocklist.flatMap { Self.comparableTerms([$0]) })
        let candidates = try entries
            .map { try $0.standardized() }
            .filter { Self.identityComparableTerms($0).isDisjoint(with: normalizedBlocklist) }

        var parents = Array(candidates.indices)
        func find(_ index: Int) -> Int {
            var current = index
            while parents[current] != current {
                parents[current] = parents[parents[current]]
                current = parents[current]
            }
            return current
        }
        func union(_ lhs: Int, _ rhs: Int) {
            let lhsRoot = find(lhs)
            let rhsRoot = find(rhs)
            guard lhsRoot != rhsRoot else { return }
            parents[max(lhsRoot, rhsRoot)] = min(lhsRoot, rhsRoot)
        }

        var ownerByTerm: [String: Int] = [:]
        for (index, entry) in candidates.enumerated() {
            for term in Self.identityComparableTerms(entry) {
                if let owner = ownerByTerm[term] {
                    union(index, owner)
                } else {
                    ownerByTerm[term] = index
                }
            }
        }

        var grouped: [Int: [SharedSponsorLexiconEntry]] = [:]
        for (index, entry) in candidates.enumerated() {
            grouped[find(index), default: []].append(entry)
        }

        return try grouped.values
            .map(Self.mergeEquivalentEntries)
            .sorted(by: SharedSponsorLexiconArtifact.entrySort)
    }

    private static func mergeEquivalentEntries(_ entries: [SharedSponsorLexiconEntry]) throws -> SharedSponsorLexiconEntry {
        let canonical = entries.sorted(by: preferredCanonicalEntry)[0]
        let alternateCanonicalNames = entries
            .map(\.canonicalName)
            .filter { SharedSponsorLexiconEntry.normalize($0) != SharedSponsorLexiconEntry.normalize(canonical.canonicalName) }
        let merged = SharedSponsorLexiconEntry(
            id: entries.map(\.id).min() ?? canonical.id,
            canonicalName: canonical.canonicalName,
            aliases: entries.flatMap(\.aliases) + alternateCanonicalNames,
            vanityURLs: entries.flatMap(\.vanityURLs),
            ctaPatternDescriptors: entries.flatMap(\.ctaPatternDescriptors),
            disclosurePhrases: entries.flatMap(\.disclosurePhrases),
            type: entries.map(\.type).sorted(by: entryTypeSort)[0],
            category: canonical.category,
            provenance: entries.count == 1 ? canonical.provenance : "central-deduped",
            version: entries.map(\.version).max() ?? canonical.version
        )
        return try merged.standardized()
    }

    private static func comparableTerms(_ terms: [String]) -> Set<String> {
        SharedSponsorLexiconTermVariants.comparableTerms(terms)
    }

    private static func identityComparableTerms(_ entry: SharedSponsorLexiconEntry) -> Set<String> {
        SharedSponsorLexiconTermVariants.identityComparableTerms(
            canonicalName: entry.canonicalName,
            aliases: entry.aliases,
            vanityURLs: entry.vanityURLs
        )
    }

    private static func preferredCanonicalEntry(
        _ lhs: SharedSponsorLexiconEntry,
        _ rhs: SharedSponsorLexiconEntry
    ) -> Bool {
        let lhsCompact = SharedSponsorLexiconEntry.compact(lhs.canonicalName)
        let rhsCompact = SharedSponsorLexiconEntry.compact(rhs.canonicalName)
        if lhsCompact != rhsCompact {
            return lhsCompact < rhsCompact
        }
        let lhsNormalized = SharedSponsorLexiconEntry.normalize(lhs.canonicalName)
        let rhsNormalized = SharedSponsorLexiconEntry.normalize(rhs.canonicalName)
        if lhsNormalized.count != rhsNormalized.count {
            return lhsNormalized.count < rhsNormalized.count
        }
        if lhsNormalized != rhsNormalized {
            return lhsNormalized < rhsNormalized
        }
        return lhs.canonicalName < rhs.canonicalName
    }

    private static func entryTypeSort(_ lhs: SharedSponsorLexiconEntryType, _ rhs: SharedSponsorLexiconEntryType) -> Bool {
        entryTypePriority(lhs) < entryTypePriority(rhs)
    }

    private static func entryTypePriority(_ type: SharedSponsorLexiconEntryType) -> Int {
        switch type {
        case .sponsor: return 0
        case .url: return 1
        case .cta: return 2
        case .disclosure: return 3
        }
    }
}
