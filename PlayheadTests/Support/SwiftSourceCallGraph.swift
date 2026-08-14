// SwiftSourceCallGraph.swift
// playhead-xul6: a SYNCHRONOUS call-graph walker over the app's own Swift
// sources, so a launch-path canary can ask a question about a CALL GRAPH
// rather than about the text of one function.
//
// WHY THIS EXISTS. `PlayheadRuntimeInitLaunchPathSourceCanaryTests`
// `.testInitBodyHasNoFoundationModelsConstruction` bans `SystemLanguageModel(`
// inside `PlayheadRuntime.init`'s body text — and it PASSED throughout the six
// weeks the launch path was holding the main actor for 0.47-2.13 s on exactly
// that API. The construction had moved ONE CALL DEEPER, into
// `CapabilitiesService.init` -> `captureSnapshot()` ->
// `checkFoundationModelsState()`, where a grep over init's own source can never
// reach. A ban on a spelling in one function is not a guard on a call graph;
// this file is the difference.
//
// WHAT IT MODELS. Starting from a named root function body, it follows only
// edges that execute SYNCHRONOUSLY when that body runs:
//
//   * `Type(...)` / `Type.init(...)`  -> that type's initialiser(s)
//   * `Type.member(...)`              -> that type's member
//   * `self.member(...)` / `Self.member(...)` / bare `member(...)`
//                                     -> the same type's member
//
// It will not follow a member declared `async`: an async member cannot run
// inside a synchronous body, and the `Task { }` that would call it is a
// different execution.
//
// CLOSURE LITERALS ARE VALUES, NOT CALLS. A `{ ... }` in expression position is
// blanked before a body is scanned, because writing a closure does not run it.
// That is what makes `Task { await ... }` and
// `BackfillJobRunner.PermissiveClassifierBox { PermissiveAdClassifier() }` —
// the two sanctioned deferral shapes on this launch path — correctly invisible
// WITHOUT an allowlist that anyone can grow. A `{ ... }` that opens a
// control-flow or declaration block IS scanned, as is one that is immediately
// applied (`{ ... }()`) or handed to a higher-order function known to invoke it
// eagerly (``eagerClosureCallees``).
//
// LIMITS, NAMED RATHER THAN IMPLIED — this is a source walker, not a compiler:
//
//   L-1  A call through a stored property or a local of inferred type
//        (`someService.doThing()`) is NOT resolved: there is no type inference
//        here. Constructions and same-type calls — the shape every launch-path
//        defect in this repo has taken — are resolved.
//   L-2  A closure handed to a function that invokes it synchronously but is
//        not in ``eagerClosureCallees`` is treated as deferred. Add the name
//        when one appears.
//   L-3  `@autoclosure` parameters are treated as deferred.
//   L-4  Member identity is `(type name, member name)`; overloads and generic
//        constraints are ignored, so a name shared by two types merges their
//        bodies. That is the OVER-approximating direction — more is scanned.
//   L-5  Protocol witnesses and subclass overrides are not resolved; a call
//        through an existential reaches nothing.
//   L-6  A member ACCESS that RUNS CODE is invisible, in both directions.
//        ``callees`` emits an edge only for `name(`, so `Self.someProperty` —
//        whose getter executes — reaches nothing; and ``indexFunctions``
//        records `func` and `init` declarations only, so a computed property's
//        body is never indexed even if something did reach it. DEMONSTRATED at
//        review by mutation: a `private static var` returning
//        `SystemLanguageModel.default.contextSize`, read from
//        `CapabilitiesService.init` as `_ = Self.thatProperty`, is a real
//        main-actor block on the launch path and this walk reports it clean.
//   L-7  A type-scope PROPERTY INITIALISER is invisible. Only the bodies of
//        `func`/`init` declarations are scanned, so
//        `private let x = Foo.expensive()` — which runs synchronously inside
//        EVERY initialiser of that type, i.e. exactly where playhead-xul6's
//        defect lived — is never seen. DEMONSTRATED at review by the same
//        mutation run.
//
// L-6 and L-7 are documented rather than closed, deliberately. Both cures cost
// the property that makes this walker worth reading — that every path it
// reports is a real one. Scanning type-scope declaration text for L-7 reads
// TYPE ANNOTATIONS as executed code: `private let model: SystemLanguageModel`
// (PermissiveAdClassifier), `private let session: LanguageModelSession` and
// `LanguageModelSession.GenerationError.Refusal` (FoundationModelClassifier)
// are three such lines in the shipped tree, each of which would report as a
// launch-path FoundationModels touch on any visit to its type. Telling an
// annotation from an expression is parsing, not scanning. Emitting parenless
// member edges for L-6 is nearer to feasible but needs the same indexer to
// tell a type-scope computed property from a local one, and this indexer
// deliberately descends into function bodies (see ``indexFunctions``). A
// canary that goes red for a non-defect is how canaries get widened.
//
// The walk is therefore an UNDER-approximation of what init really executes,
// and every path it reports is a real synchronous one. Its value is that the
// paths it does cover are the ones this repo keeps regressing on, and that a
// caller can assert the walk REACHED a named node (``Result/visited``) — so a
// resolver that silently stops working fails the canary instead of quietly
// turning it green.
//
// Implementation note: everything below works on UTF-8 BYTES rather than
// `Character`s. The production tree is ~12 MB of Swift, and the
// Character-indexed helpers in ``SwiftSourceInspector`` are far too slow for
// that in a debug test build.

import Foundation

enum SwiftSourceCallGraph {

    // MARK: - Public surface

    /// One node of the graph: a member of a named type.
    struct Node: Hashable, CustomStringConvertible {
        let type: String
        /// `"init"` for an initialiser, otherwise the function name.
        let member: String
        var description: String { "\(type).\(member)" }
    }

    /// One synchronous path from the root to a body mentioning a banned token.
    struct Finding: CustomStringConvertible {
        let token: String
        let path: [Node]
        var description: String {
            path.map(\.description).joined(separator: " -> ") + "   [\(token)]"
        }
    }

    struct Result {
        /// Every node the walk entered, including the root.
        let visited: Set<Node>
        /// Paths whose final body mentions one of the banned tokens.
        let findings: [Finding]
        /// Byte length of the root body — a non-vacuity handle for callers.
        let rootBodyByteCount: Int
        /// How many `(type, member)` pairs the index resolved across the tree.
        let indexedMemberCount: Int
        /// How many `.swift` files were indexed.
        let indexedFileCount: Int
    }

    /// Higher-order functions that invoke their closure argument
    /// SYNCHRONOUSLY. A closure handed to one of these is scanned rather than
    /// blanked. See L-2.
    static let eagerClosureCallees: Set<String> = [
        "forEach", "map", "compactMap", "flatMap", "filter", "reduce", "sorted",
        "first", "last", "contains", "allSatisfy", "min", "max", "partition",
        "autoreleasepool", "withoutActuallyEscaping", "withUnsafePointer",
        "withUnsafeBytes", "withUnsafeMutableBytes", "sync", "perform",
        "removeAll", "firstIndex", "lastIndex", "drop", "prefix",
    ]

    enum Failure: Error, CustomStringConvertible {
        case repositoryRootNotFound(String)
        case rootFileNotIndexed(String)
        case rootSignatureNotFound(String, String)

        var description: String {
            switch self {
            case .repositoryRootNotFound(let path):
                return "could not locate the repository root from \(path)"
            case .rootFileNotIndexed(let path):
                return "the root file \(path) is not under the indexed source directory"
            case .rootSignatureNotFound(let signature, let path):
                return """
                the root signature `\(signature)` was not found in \(path). The canary must be \
                updated alongside any rename — a silently-missing root is exactly the vacuity \
                this walker exists to remove.
                """
            }
        }
    }

    /// Walks the synchronous call graph rooted at the body following
    /// `rootSignature` in `rootRelativePath`, reporting every path whose body
    /// mentions one of `bannedTokens`.
    static func walk(
        sourceRoot: String,
        rootRelativePath: String,
        rootType: String,
        rootSignature: String,
        bannedTokens: [String],
        filePath: String = #filePath
    ) throws -> Result {
        guard let repoRoot = SwiftSourceInspector.repositoryRoot(from: filePath) else {
            throw Failure.repositoryRootNotFound(filePath)
        }
        let index = try MemberIndex(directory: repoRoot.appendingPathComponent(sourceRoot))
        let rootPath = repoRoot.appendingPathComponent(rootRelativePath).path
        guard let rootBytes = index.bytes(ofFileAt: rootPath) else {
            throw Failure.rootFileNotIndexed(rootRelativePath)
        }
        guard let rootBody = body(in: rootBytes, afterSignature: rootSignature) else {
            throw Failure.rootSignatureNotFound(rootSignature, rootRelativePath)
        }

        let banned = bannedTokens.map { Array($0.utf8) }
        let rootNode = Node(type: rootType, member: "init")
        var visited: Set<Node> = [rootNode]
        var findings: [Finding] = []
        var queue: [(node: Node, body: [UInt8], path: [Node])] = [(rootNode, rootBody, [rootNode])]
        var head = 0

        while head < queue.count {
            let item = queue[head]
            head += 1
            let scanned = synchronousText(of: item.body)
            var hitToken: [UInt8]?
            for token in banned where hitToken == nil {
                if containsWord(token, in: scanned) { hitToken = token }
            }
            if let hitToken {
                findings.append(
                    Finding(token: String(decoding: hitToken, as: UTF8.self), path: item.path)
                )
                continue
            }
            for callee in callees(in: scanned, owner: item.node.type) {
                if visited.contains(callee) { continue }
                guard let bodies = index.bodies(of: callee), !bodies.isEmpty else { continue }
                visited.insert(callee)
                for calleeBody in bodies {
                    queue.append((callee, calleeBody, item.path + [callee]))
                }
            }
        }

        return Result(
            visited: visited,
            findings: findings,
            rootBodyByteCount: rootBody.count,
            indexedMemberCount: index.memberCount,
            indexedFileCount: index.fileCount
        )
    }

    // MARK: - Byte primitives

    static func isWordByte(_ b: UInt8) -> Bool {
        (b >= 0x30 && b <= 0x39) || (b >= 0x41 && b <= 0x5A)
            || (b >= 0x61 && b <= 0x7A) || b == UInt8(ascii: "_")
    }

    static func isUpperByte(_ b: UInt8) -> Bool { b >= 0x41 && b <= 0x5A }

    static func isSpaceByte(_ b: UInt8) -> Bool {
        b == UInt8(ascii: " ") || b == UInt8(ascii: "\t") || b == UInt8(ascii: "\n")
            || b == UInt8(ascii: "\r")
    }

    /// Whole-word containment over bytes.
    static func containsWord(_ word: [UInt8], in bytes: [UInt8]) -> Bool {
        guard !word.isEmpty, bytes.count >= word.count else { return false }
        let limit = bytes.count - word.count
        var i = 0
        while i <= limit {
            if bytes[i] == word[0] {
                var match = true
                var k = 1
                while k < word.count {
                    if bytes[i + k] != word[k] {
                        match = false
                        break
                    }
                    k += 1
                }
                if match {
                    let beforeOK = i == 0 || !isWordByte(bytes[i - 1])
                    let after = i + word.count
                    let afterOK = after >= bytes.count || !isWordByte(bytes[after])
                    if beforeOK && afterOK { return true }
                }
            }
            i += 1
        }
        return false
    }

    /// Index of the `}` matching the `{` at `open`, or `nil` when unbalanced.
    /// Assumes `bytes` has already been comment/string-blanked.
    static func matchingBrace(in bytes: [UInt8], open: Int) -> Int? {
        var depth = 0
        var i = open
        while i < bytes.count {
            if bytes[i] == UInt8(ascii: "{") {
                depth += 1
            } else if bytes[i] == UInt8(ascii: "}") {
                depth -= 1
                if depth == 0 { return i }
            }
            i += 1
        }
        return nil
    }

    /// The brace-delimited body that follows the first literal occurrence of
    /// `signature`, or `nil`.
    static func body(in bytes: [UInt8], afterSignature signature: String) -> [UInt8]? {
        let needle = Array(signature.utf8)
        guard let start = firstIndex(of: needle, in: bytes) else { return nil }
        var i = start
        while i < bytes.count, bytes[i] != UInt8(ascii: "{") { i += 1 }
        guard i < bytes.count, let close = matchingBrace(in: bytes, open: i) else { return nil }
        return Array(bytes[(i + 1)..<close])
    }

    static func firstIndex(of needle: [UInt8], in haystack: [UInt8], from: Int = 0) -> Int? {
        guard !needle.isEmpty, haystack.count >= needle.count else { return nil }
        let limit = haystack.count - needle.count
        var i = max(0, from)
        while i <= limit {
            if haystack[i] == needle[0] {
                var match = true
                var k = 1
                while k < needle.count {
                    if haystack[i + k] != needle[k] {
                        match = false
                        break
                    }
                    k += 1
                }
                if match { return i }
            }
            i += 1
        }
        return nil
    }

    /// Returns `bytes` with the CONTENTS of every comment and string literal
    /// replaced by spaces (newlines preserved, so byte offsets are stable).
    /// Handles `//`, nested `/* */`, `"..."`, `"""..."""` and raw `#"..."#`.
    static func blankingCommentsAndStrings(_ bytes: [UInt8]) -> [UInt8] {
        var out = bytes
        let n = bytes.count
        var i = 0

        func blank(_ from: Int, _ to: Int) {
            var k = max(0, from)
            let end = min(to, n)
            while k < end {
                if out[k] != UInt8(ascii: "\n") { out[k] = UInt8(ascii: " ") }
                k += 1
            }
        }

        while i < n {
            let c = bytes[i]
            if c == UInt8(ascii: "/"), i + 1 < n, bytes[i + 1] == UInt8(ascii: "/") {
                var j = i
                while j < n, bytes[j] != UInt8(ascii: "\n") { j += 1 }
                blank(i, j)
                i = j
                continue
            }
            if c == UInt8(ascii: "/"), i + 1 < n, bytes[i + 1] == UInt8(ascii: "*") {
                var depth = 1
                var j = i + 2
                while j < n, depth > 0 {
                    if bytes[j] == UInt8(ascii: "/"), j + 1 < n, bytes[j + 1] == UInt8(ascii: "*") {
                        depth += 1
                        j += 2
                    } else if bytes[j] == UInt8(ascii: "*"), j + 1 < n,
                              bytes[j + 1] == UInt8(ascii: "/") {
                        depth -= 1
                        j += 2
                    } else {
                        j += 1
                    }
                }
                blank(i, j)
                i = min(j, n)
                continue
            }
            if c == UInt8(ascii: "#") || c == UInt8(ascii: "\"") {
                var hashes = 0
                var j = i
                while j < n, bytes[j] == UInt8(ascii: "#") {
                    hashes += 1
                    j += 1
                }
                guard j < n, bytes[j] == UInt8(ascii: "\"") else {
                    i += 1
                    continue
                }
                let triple = j + 2 < n && bytes[j + 1] == UInt8(ascii: "\"")
                    && bytes[j + 2] == UInt8(ascii: "\"")
                let quoteRun = triple ? 3 : 1
                var k = j + quoteRun
                var closed = false
                while k < n {
                    if hashes == 0, bytes[k] == UInt8(ascii: "\\"), k + 1 < n {
                        k += 2
                        continue
                    }
                    if bytes[k] == UInt8(ascii: "\""), closerMatches(
                        bytes, at: k, quoteRun: quoteRun, hashes: hashes
                    ) {
                        blank(i, k + quoteRun + hashes)
                        i = k + quoteRun + hashes
                        closed = true
                        break
                    }
                    k += 1
                }
                if !closed {
                    blank(i, n)
                    i = n
                }
                continue
            }
            i += 1
        }
        return out
    }

    private static func closerMatches(
        _ bytes: [UInt8], at k: Int, quoteRun: Int, hashes: Int
    ) -> Bool {
        var q = 0
        while q < quoteRun {
            if k + q >= bytes.count || bytes[k + q] != UInt8(ascii: "\"") { return false }
            q += 1
        }
        var h = 0
        while h < hashes {
            if k + quoteRun + h >= bytes.count || bytes[k + quoteRun + h] != UInt8(ascii: "#") {
                return false
            }
            h += 1
        }
        return true
    }

    // MARK: - Deferred-closure blanking

    /// Keywords whose presence in the statement preceding a `{` means the block
    /// is executed in place rather than being a closure VALUE.
    private static let controlKeywords: [[UInt8]] = [
        "if", "guard", "else", "for", "while", "switch", "do", "catch",
        "defer", "repeat", "func", "init", "deinit",
    ].map { Array($0.utf8) }

    /// Blanks every deferred closure literal, leaving control-flow blocks,
    /// immediately-applied closures and eager-callee closures intact.
    static func synchronousText(of body: [UInt8]) -> [UInt8] {
        var out = body
        scanRegion(body, &out, from: 0, to: body.count)
        return out
    }

    private static func matchesKeyword(_ bytes: [UInt8], _ lo: Int, _ hi: Int) -> Bool {
        let length = hi - lo
        for keyword in controlKeywords where keyword.count == length {
            var k = 0
            var equal = true
            while k < length {
                if bytes[lo + k] != keyword[k] {
                    equal = false
                    break
                }
                k += 1
            }
            if equal { return true }
        }
        return false
    }

    private static func scanRegion(
        _ body: [UInt8],
        _ out: inout [UInt8],
        from lo: Int,
        to hi: Int
    ) {
        var i = lo
        var sawControlKeyword = false
        while i < hi {
            let c = body[i]
            if isWordByte(c) {
                var j = i
                while j < hi, isWordByte(body[j]) { j += 1 }
                // `#if` / `#available` are directives, not control flow: the
                // `if` in `#if canImport(FoundationModels)` must not promote
                // the next `{` to an executed block.
                if !(i > 0 && body[i - 1] == UInt8(ascii: "#")), matchesKeyword(body, i, j) {
                    sawControlKeyword = true
                }
                i = j
                continue
            }
            if c == UInt8(ascii: ";") || c == UInt8(ascii: "}") {
                sawControlKeyword = false
                i += 1
                continue
            }
            if c == UInt8(ascii: "{") {
                guard let close = matchingBrace(in: body, open: i) else { return }
                let end = close + 1
                var k = end
                while k < body.count, isSpaceByte(body[k]) { k += 1 }
                let applied = k < body.count && body[k] == UInt8(ascii: "(")
                let eager = eagerClosureCallees.contains(trailingCalleeName(body, before: i))
                if sawControlKeyword || applied || eager {
                    scanRegion(body, &out, from: i + 1, to: close)
                } else {
                    var t = i
                    while t < end {
                        if out[t] != UInt8(ascii: "\n") { out[t] = UInt8(ascii: " ") }
                        t += 1
                    }
                }
                i = end
                sawControlKeyword = false
                continue
            }
            i += 1
        }
    }

    /// The name of the function a trailing closure at `brace` is handed to
    /// (`items.forEach {` -> `forEach`), or `""`.
    private static func trailingCalleeName(_ body: [UInt8], before brace: Int) -> String {
        var i = brace - 1
        while i >= 0, isSpaceByte(body[i]) { i -= 1 }
        if i >= 0, body[i] == UInt8(ascii: ")") {
            var depth = 0
            while i >= 0 {
                if body[i] == UInt8(ascii: ")") {
                    depth += 1
                } else if body[i] == UInt8(ascii: "(") {
                    depth -= 1
                    if depth == 0 { break }
                }
                i -= 1
            }
            i -= 1
            while i >= 0, isSpaceByte(body[i]) { i -= 1 }
        }
        guard i >= 0, isWordByte(body[i]) else { return "" }
        var j = i
        while j >= 0, isWordByte(body[j]) { j -= 1 }
        return String(decoding: body[(j + 1)...i], as: UTF8.self)
    }

    // MARK: - Callee extraction

    /// Every callee this (already closure-blanked) body invokes synchronously.
    /// See L-1 for what is deliberately not resolved.
    static func callees(in body: [UInt8], owner: String) -> [Node] {
        var result: Set<Node> = []
        let n = body.count
        var i = 0
        while i < n {
            guard isWordByte(body[i]) else {
                i += 1
                continue
            }
            let precededByDot = i > 0 && body[i - 1] == UInt8(ascii: ".")
            var j = i
            while j < n, isWordByte(body[j]) { j += 1 }
            if precededByDot {
                i = j
                continue
            }
            var k = j
            while k < n, isSpaceByte(body[k]) { k += 1 }
            guard k < n else { break }
            if body[k] == UInt8(ascii: "(") {
                let word = String(decoding: body[i..<j], as: UTF8.self)
                if isUpperByte(body[i]) {
                    result.insert(Node(type: word, member: "init"))
                } else {
                    result.insert(Node(type: owner, member: word))
                }
                i = j
                continue
            }
            if body[k] == UInt8(ascii: "."), isUpperByte(body[i]) || isSelfWord(body, i, j) {
                var m = k + 1
                while m < n, isSpaceByte(body[m]) { m += 1 }
                var p = m
                while p < n, isWordByte(body[p]) { p += 1 }
                if p > m {
                    var q = p
                    while q < n, isSpaceByte(body[q]) { q += 1 }
                    if q < n, body[q] == UInt8(ascii: "(") {
                        let member = String(decoding: body[m..<p], as: UTF8.self)
                        if isSelfWord(body, i, j) {
                            result.insert(Node(type: owner, member: member))
                        } else {
                            result.insert(
                                Node(type: String(decoding: body[i..<j], as: UTF8.self),
                                     member: member)
                            )
                        }
                    }
                }
            }
            i = j
        }
        return Array(result)
    }

    private static func isSelfWord(_ bytes: [UInt8], _ lo: Int, _ hi: Int) -> Bool {
        guard hi - lo == 4 else { return false }
        let tail = bytes[lo + 1] == UInt8(ascii: "e") && bytes[lo + 2] == UInt8(ascii: "l")
            && bytes[lo + 3] == UInt8(ascii: "f")
        return tail && (bytes[lo] == UInt8(ascii: "s") || bytes[lo] == UInt8(ascii: "S"))
    }

    // MARK: - Member index

    /// Comment/string-blanked production sources plus every `(type, member)`
    /// body a walk can resolve.
    struct MemberIndex {
        private var files: [String: [UInt8]] = [:]
        private var members: [Node: [[UInt8]]] = [:]

        init(directory: URL) throws {
            let enumerator = FileManager.default.enumerator(
                at: directory,
                includingPropertiesForKeys: nil
            )
            var urls: [URL] = []
            while let next = enumerator?.nextObject() as? URL {
                if next.pathExtension == "swift" { urls.append(next) }
            }
            for url in urls {
                let raw = try Data(contentsOf: url)
                let blanked = SwiftSourceCallGraph.blankingCommentsAndStrings([UInt8](raw))
                files[url.path] = blanked
                SwiftSourceCallGraph.indexTypes(in: blanked, into: &members)
            }
        }

        func bytes(ofFileAt path: String) -> [UInt8]? { files[path] }
        func bodies(of node: Node) -> [[UInt8]]? { members[node] }
        var memberCount: Int { members.count }
        var fileCount: Int { files.count }
    }

    private static let typeKeywords: [[UInt8]] = ["actor", "class", "struct", "enum", "extension"]
        .map { Array($0.utf8) }
    private static let funcKeyword = Array("func".utf8)
    private static let initKeyword = Array("init".utf8)
    private static let asyncKeyword = Array("async".utf8)

    private static func wordMatches(_ bytes: [UInt8], _ lo: Int, _ hi: Int, _ word: [UInt8]) -> Bool {
        guard hi - lo == word.count else { return false }
        var k = 0
        while k < word.count {
            if bytes[lo + k] != word[k] { return false }
            k += 1
        }
        return true
    }

    /// Records every `(type, member) -> body` declared in one blanked file.
    /// Nested types are also indexed under their own name AND folded into the
    /// enclosing type's body — the over-approximating direction (L-4).
    static func indexTypes(in bytes: [UInt8], into members: inout [Node: [[UInt8]]]) {
        let n = bytes.count
        var i = 0
        while i < n {
            guard isWordByte(bytes[i]) else {
                i += 1
                continue
            }
            var j = i
            while j < n, isWordByte(bytes[j]) { j += 1 }
            var isTypeKeyword = false
            for keyword in typeKeywords where wordMatches(bytes, i, j, keyword) {
                isTypeKeyword = true
            }
            guard isTypeKeyword else {
                i = j
                continue
            }
            var k = j
            while k < n, isSpaceByte(bytes[k]) { k += 1 }
            guard k < n, isUpperByte(bytes[k]) else {
                i = j
                continue
            }
            var p = k
            while p < n, isWordByte(bytes[p]) { p += 1 }
            let typeName = String(decoding: bytes[k..<p], as: UTF8.self)
            var brace = p
            while brace < n, bytes[brace] != UInt8(ascii: "{") { brace += 1 }
            guard brace < n, let close = matchingBrace(in: bytes, open: brace) else {
                i = p
                continue
            }
            let typeBody = Array(bytes[(brace + 1)..<close])
            indexFunctions(in: typeBody, type: typeName, into: &members)
            // Continue INSIDE the body so nested declarations are seen too.
            i = brace + 1
        }
    }

    private static func indexFunctions(
        in typeBody: [UInt8],
        type: String,
        into members: inout [Node: [[UInt8]]]
    ) {
        let n = typeBody.count
        var i = 0
        while i < n {
            guard isWordByte(typeBody[i]) else {
                i += 1
                continue
            }
            var j = i
            while j < n, isWordByte(typeBody[j]) { j += 1 }
            let isFunc = wordMatches(typeBody, i, j, funcKeyword)
            let isInit = wordMatches(typeBody, i, j, initKeyword)
            // `.init(` is a CALL, not a declaration.
            let afterDot = i > 0 && typeBody[i - 1] == UInt8(ascii: ".")
            guard isFunc || isInit, !afterDot else {
                i = j
                continue
            }
            var name = "init"
            var cursor = j
            if isFunc {
                while cursor < n, isSpaceByte(typeBody[cursor]) { cursor += 1 }
                guard cursor < n, isWordByte(typeBody[cursor]) else {
                    i = j
                    continue
                }
                var e = cursor
                while e < n, isWordByte(typeBody[e]) { e += 1 }
                name = String(decoding: typeBody[cursor..<e], as: UTF8.self)
                cursor = e
            }
            // Optional `?`/`!` on a failable init, then an optional generic list.
            while cursor < n, typeBody[cursor] == UInt8(ascii: "?")
                || typeBody[cursor] == UInt8(ascii: "!") {
                cursor += 1
            }
            if cursor < n, typeBody[cursor] == UInt8(ascii: "<") {
                var depth = 0
                while cursor < n {
                    if typeBody[cursor] == UInt8(ascii: "<") { depth += 1 }
                    if typeBody[cursor] == UInt8(ascii: ">") {
                        depth -= 1
                        if depth == 0 {
                            cursor += 1
                            break
                        }
                    }
                    cursor += 1
                }
            }
            guard cursor < n, typeBody[cursor] == UInt8(ascii: "(") else {
                i = j
                continue
            }
            var depth = 0
            var paren = cursor
            while paren < n {
                if typeBody[paren] == UInt8(ascii: "(") { depth += 1 }
                if typeBody[paren] == UInt8(ascii: ")") {
                    depth -= 1
                    if depth == 0 { break }
                }
                paren += 1
            }
            guard paren < n else {
                i = j
                continue
            }
            var brace = paren
            while brace < n, typeBody[brace] != UInt8(ascii: "{") { brace += 1 }
            guard brace < n, let close = matchingBrace(in: typeBody, open: brace) else {
                i = j
                continue
            }
            // An `async` member cannot run inside a synchronous body.
            let signatureTail = Array(typeBody[paren..<brace])
            if containsWord(asyncKeyword, in: signatureTail) {
                i = brace + 1
                continue
            }
            members[Node(type: type, member: name), default: []]
                .append(Array(typeBody[(brace + 1)..<close]))
            i = brace + 1
        }
    }
}
