import Foundation
import Testing

@testable import Playhead

/// playhead-1shd — the 5th instance of the sceneless-launch defect class.
///
/// The coarse-scan / final-pass show-identity resolvers
/// (`setEpisodePodcastId*Resolver`) were installed ONLY from the WindowGroup
/// `.task`, a SwiftUI scene modifier. A `BGAppRefreshTask` / BGProcessingTask
/// wake has NO scene, so `.task` never ran, `PlayheadRuntime`'s resolver box
/// stayed `nil`, and `AnalysisCoordinator.runPendingCoarseScans` (whose sole
/// caller is the BGTask handler registered from `PlayheadRuntime.init`) logged
/// `coarse_scan_show_identity_unknown … resolverInstalled=false` and refused
/// the claim with `scan_claim:podcast_id_missing`. The final-pass launch sweep,
/// which polls the same box, wrote a null `FinalPassJob.podcastId` on the same
/// launches.
///
/// The fix hoists the install into `PlayheadApp.init()` — a scene-INDEPENDENT
/// hook that runs on every launch, headless or not, and where `modelContainer`
/// (the closures' only capture) already exists. The scene's `.task` keeps an
/// idempotent SECOND write so a normal launch is unchanged.
///
/// This is a SOURCE canary: it reads `PlayheadApp.swift` and asserts the install
/// call sits in the `init()` body (not only in scene scope). It reddens if a
/// future refactor moves the install back into `.task` only.
@Suite("Show-identity resolver installs on every launch (playhead-1shd)")
struct ShowIdentityResolverEveryLaunchCanaryTests {

    private static let installMarker = "installEpisodePodcastIdResolversForEveryLaunch"

    private static func appSource() throws -> String {
        let root = try #require(
            SwiftSourceInspector.repositoryRoot(from: #filePath),
            "could not locate the repository root from \(#filePath)"
        )
        let url = root.appendingPathComponent("Playhead/App/PlayheadApp.swift")
        return try String(contentsOf: url, encoding: .utf8)
    }

    private static func sceneBodyOffset(_ source: String) throws -> String.Index {
        try #require(
            source.range(of: "var body: some Scene")?.lowerBound,
            "PlayheadApp no longer declares `var body: some Scene` — re-read this canary"
        )
    }

    /// The region strictly between `init() {` and its matching close brace. The
    /// static installer is declared AFTER `init`, so this region contains only
    /// the CALL, never the definition — which is what makes the acceptance below
    /// non-vacuous.
    private static func initBody(_ source: String) throws -> Substring {
        let start = try #require(
            source.range(of: "init() {"),
            "PlayheadApp no longer has an `init() {`"
        )
        var depth = 1
        var index = start.upperBound
        while index < source.endIndex, depth > 0 {
            if source[index] == "{" { depth += 1 }
            if source[index] == "}" { depth -= 1 }
            if depth == 0 { break }
            index = source.index(after: index)
        }
        #expect(depth == 0, "init's braces do not balance — re-read this canary")
        return source[start.upperBound..<index]
    }

    @Test("THE ACCEPTANCE: init installs the show-identity resolvers (playhead-1shd)")
    func initInstallsTheResolvers() throws {
        let initRegion = try Self.initBody(Self.appSource())

        #expect(
            initRegion.contains(Self.installMarker),
            """
            PlayheadApp.init no longer installs the show-identity resolvers. A \
            BGTask wake has NO SCENE, so an install that happens only in `.task` \
            leaves PlayheadRuntime's resolver box nil on exactly the launches \
            the coarse-scan / final-pass sweep runs on — and the show identity \
            is refused with `scan_claim:podcast_id_missing` (playhead-1shd, the \
            5th sceneless-launch instance).
            """
        )
    }

    @Test("the install is NOT scene-only: it appears in init, before `var body` (playhead-1shd)")
    func installIsNotSceneOnly() throws {
        let source = try Self.appSource()
        let initRegion = try Self.initBody(source)

        // The initBody region ends before `var body: some Scene`, so a match
        // here is proof the CALL is in init — not merely that the function is
        // DECLARED somewhere above the scene.
        #expect(
            initRegion.contains(Self.installMarker),
            """
            The only install left is inside scene scope. That is the \
            sceneless-launch defect exactly: correct on a normal launch, inert \
            on a headless one.
            """
        )
        // Guard: the marker in init must be a CALL, not the func declaration
        // having drifted up into init.
        #expect(
            !initRegion.contains("static func \(Self.installMarker)"),
            "the installer DEFINITION has moved inside init — this canary is no longer isolating the call site"
        )
    }

    @Test("the scene `.task` keeps an idempotent second write (playhead-1shd)")
    func sceneSecondWritePreserved() throws {
        let source = try Self.appSource()
        let sceneScope = source[try Self.sceneBodyOffset(source)...]

        #expect(
            sceneScope.contains(Self.installMarker),
            """
            The scene path must still install the resolvers as an idempotent \
            second write, so a normal (scene'd) launch is unchanged. The setter \
            overwrites the box under a lock, so re-installing is a safe no-op.
            """
        )
    }

    @Test("init installs WITHOUT reaching into scene-only state (playhead-1shd)")
    func initInstallStandsAlone() throws {
        let initRegion = try Self.initBody(Self.appSource())

        // The init install must use `PlayheadRuntime.shared` (the process
        // singleton), never the `@State` runtime, which is not safe to touch in
        // init. If a refactor made init read `@State`, the app would crash on
        // launch long before this string check mattered — but pinning the
        // source keeps the documented `.shared` route visible.
        #expect(
            initRegion.contains("\(Self.installMarker)(") ,
            "the init region no longer CALLS the installer"
        )
        #expect(
            initRegion.contains("runtime: PlayheadRuntime.shared"),
            "the init install must pass the process singleton, not @State runtime"
        )
    }

    @Test("the canary is reading the file it thinks it is (playhead-1shd)")
    func canaryReadsTheRealSource() throws {
        let source = try Self.appSource()
        #expect(source.contains("struct PlayheadApp"))
        #expect(source.contains("var body: some Scene"))
        #expect(source.count > 5_000, "a truncated read would pass every check above")
        let initRegion = try Self.initBody(source)
        #expect(!initRegion.isEmpty)
        #expect(
            !initRegion.contains("var body: some Scene"),
            "initBody has swallowed the scene declaration — it is not isolating init"
        )
        #expect(
            !initRegion.contains("static func \(Self.installMarker)"),
            "initBody has swallowed the installer DECLARATION, which is how this rail would pass with the defect in place"
        )
    }
}
