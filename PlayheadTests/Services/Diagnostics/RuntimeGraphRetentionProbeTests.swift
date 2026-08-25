import Foundation
import Testing
@testable import Playhead

/// playhead-882eg PROBE (temporary; not a rail yet).
///
/// The test host holds a descriptor FLOOR of ~453 for the whole tail of every
/// full-plan run, of which 163 are on the PRODUCTION `analysis.sqlite`, 153 on
/// the production `ad_catalog.sqlite` and 81 on distinct
/// `surface-status-*.jsonl` files. This probe asks the one question the fd
/// dump cannot answer: WHICH object is retained — the `PlayheadRuntime`, or a
/// store that outlives it?
enum FDProbe {
    /// Count open descriptors whose `F_GETPATH` contains `needle`.
    static func count(containing needle: String) -> Int {
        var found = 0
        let table = Int32(getdtablesize())
        var buf = [CChar](repeating: 0, count: Int(PATH_MAX))
        for fd in 0..<table {
            let rc = buf.withUnsafeMutableBufferPointer { p -> Int32 in
                fcntl(fd, F_GETPATH, p.baseAddress)
            }
            guard rc != -1 else { continue }
            let path = String(cString: buf)
            if path.contains(needle) { found += 1 }
        }
        return found
    }

    static func total() -> Int {
        var found = 0
        let table = Int32(getdtablesize())
        for fd in 0..<table where fcntl(fd, F_GETFD) != -1 { found += 1 }
        return found
    }
}

@MainActor
@Suite("playhead-882eg probe: what survives a dropped PlayheadRuntime", .serialized)
struct RuntimeGraphRetentionProbeTests {
    private func drain(_ seconds: Double) async {
        let deadline = Date().addingTimeInterval(seconds)
        while Date() < deadline {
            try? await Task.sleep(for: .milliseconds(100))
        }
    }

    @Test("preview runtime, no shutdown", .timeLimit(.minutes(2)))
    func previewNoShutdown() async throws {
        let before = (
            fds: FDProbe.total(),
            analysis: FDProbe.count(containing: "AnalysisStore/analysis.sqlite"),
            catalog: FDProbe.count(containing: "ad_catalog.sqlite"),
            surface: FDProbe.count(containing: "surface-status-")
        )
        weak var weakRuntime: PlayheadRuntime?
        weak var weakStore: AnalysisStore?
        weak var weakSurface: SurfaceStatusInvariantLogger?
        await {
            let runtime = PlayheadRuntime(isPreviewRuntime: true)
            weakRuntime = runtime
            weakStore = runtime.analysisStore
            weakSurface = runtime.surfaceStatusLogger
        }()
        await drain(6)
        let after = (
            fds: FDProbe.total(),
            analysis: FDProbe.count(containing: "AnalysisStore/analysis.sqlite"),
            catalog: FDProbe.count(containing: "ad_catalog.sqlite"),
            surface: FDProbe.count(containing: "surface-status-")
        )
        print("""
        [882eg-probe] previewNoShutdown \
        runtime=\(weakRuntime == nil ? "released" : "RETAINED") \
        analysisStore=\(weakStore == nil ? "released" : "RETAINED") \
        surfaceLogger=\(weakSurface == nil ? "released" : "RETAINED") \
        fds \(before.fds)->\(after.fds) \
        analysis \(before.analysis)->\(after.analysis) \
        catalog \(before.catalog)->\(after.catalog) \
        surface \(before.surface)->\(after.surface)
        """)
    }

    @Test("preview runtime, WITH shutdown", .timeLimit(.minutes(2)))
    func previewWithShutdown() async throws {
        let before = (
            fds: FDProbe.total(),
            analysis: FDProbe.count(containing: "AnalysisStore/analysis.sqlite"),
            catalog: FDProbe.count(containing: "ad_catalog.sqlite"),
            surface: FDProbe.count(containing: "surface-status-")
        )
        weak var weakRuntime: PlayheadRuntime?
        weak var weakStore: AnalysisStore?
        weak var weakSurface: SurfaceStatusInvariantLogger?
        await {
            let runtime = PlayheadRuntime(isPreviewRuntime: true)
            weakRuntime = runtime
            weakStore = runtime.analysisStore
            weakSurface = runtime.surfaceStatusLogger
            await runtime.shutdown()
        }()
        await drain(6)
        let after = (
            fds: FDProbe.total(),
            analysis: FDProbe.count(containing: "AnalysisStore/analysis.sqlite"),
            catalog: FDProbe.count(containing: "ad_catalog.sqlite"),
            surface: FDProbe.count(containing: "surface-status-")
        )
        print("""
        [882eg-probe] previewWithShutdown \
        runtime=\(weakRuntime == nil ? "released" : "RETAINED") \
        analysisStore=\(weakStore == nil ? "released" : "RETAINED") \
        surfaceLogger=\(weakSurface == nil ? "released" : "RETAINED") \
        fds \(before.fds)->\(after.fds) \
        analysis \(before.analysis)->\(after.analysis) \
        catalog \(before.catalog)->\(after.catalog) \
        surface \(before.surface)->\(after.surface)
        """)
    }

    @Test("five preview runtimes in a row", .timeLimit(.minutes(3)))
    func fivePreviewRuntimes() async throws {
        let before = (
            fds: FDProbe.total(),
            analysis: FDProbe.count(containing: "AnalysisStore/analysis.sqlite"),
            catalog: FDProbe.count(containing: "ad_catalog.sqlite"),
            surface: FDProbe.count(containing: "surface-status-")
        )
        for _ in 0..<5 {
            await {
                _ = PlayheadRuntime(isPreviewRuntime: true)
            }()
        }
        await drain(8)
        let after = (
            fds: FDProbe.total(),
            analysis: FDProbe.count(containing: "AnalysisStore/analysis.sqlite"),
            catalog: FDProbe.count(containing: "ad_catalog.sqlite"),
            surface: FDProbe.count(containing: "surface-status-")
        )
        print("""
        [882eg-probe] fivePreviewRuntimes \
        fds \(before.fds)->\(after.fds) \
        analysis \(before.analysis)->\(after.analysis) \
        catalog \(before.catalog)->\(after.catalog) \
        surface \(before.surface)->\(after.surface)
        """)
    }
}
