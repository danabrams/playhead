// swift-tools-version: 6.0

// Copyright 2026 Apple Inc.
//
// Use of this source code is governed by a BSD-3-clause license that can
// be found in the LICENSE file or at https://opensource.org/licenses/BSD-3-Clause

// ---------------------------------------------------------------------------
// Playhead vendored fork (playhead-b6jq PR 1) of apple/coreai-models.
//
// Trimmed to ONLY what the `CoreAILM` product needs:
//   CoreAILanguageModels + CoreAIShared + CXGrammar.
// Diffusion / segmentation / speech / object-detection products, all
// executable tools, and all test targets are removed (their sources are
// not vendored).
//
// CRITICAL deviation from upstream — `exclude:` on CoreAILanguageModels:
// the four FoundationModels-bridge files are excluded from compilation.
// Upstream compiles them unconditionally; linking them pulls in
// FoundationModels adapter symbols that are not present on shipped iOS
// seeds and dyld-crashes the app at launch (proven in the coreai-spike
// phase-B probe). The files stay in the tree (upstream parity, future
// device-only bridging) but MUST remain excluded here.
//
// Dependency pins (upstream uses branch:"main" for xgrammar — a
// reproducibility hazard): both remote deps are pinned to the exact
// state resolved in the spike's Package.resolved.
//
// Simulator support: the CoreAI framework ships only in DEVICE SDKs
// (absent from the iOS simulator SDK), so every CoreAI-importing source
// file in this fork carries a `#if canImport(CoreAI)` whole-file guard
// (plus a partial guard around the multimodal protocol in
// InferenceEngine.swift). On simulator the module still builds and is
// importable — with the engine surface compiled out. Device builds are
// unchanged from upstream.
// ---------------------------------------------------------------------------

import PackageDescription

let package = Package(
    name: "coreai-models",
    platforms: [.macOS("27.0"), .iOS("27.0")],
    products: [
        .library(
            name: "CoreAILM",
            targets: [
                "CoreAILanguageModels"
            ]
        )
    ],
    dependencies: [
        // Spike-resolved 1.2.0 (revision eed7264ac5e4ec5dfa6165c6e5c5577364344fe4).
        .package(url: "https://github.com/huggingface/swift-transformers", exact: "1.2.0"),
        // Upstream pins branch:"main"; we pin the spike-resolved revision.
        .package(url: "https://github.com/mlc-ai/xgrammar", revision: "4d145cc13d878c751ebeed36af1c013074be76bc"),
    ],
    targets: [
        .target(
            name: "CoreAILanguageModels",
            dependencies: [
                "CoreAIShared",
                "CXGrammar",
                .product(name: "Transformers", package: "swift-transformers"),
            ],
            path: "swift/Sources/CoreAILanguageModels",
            exclude: [
                // FoundationModels-bridge files — see header comment.
                "LanguageModel/CoreAILanguageModel.swift",
                "LanguageModel/CoreAIRunner.swift",
                "VLM/CoreAIVisionLanguageModel.swift",
                "LanguageModel/ModelResources.swift",
            ],
            swiftSettings: [
                .define("CXGRAMMAR_IMPORT"),
                .enableUpcomingFeature("MemberImportVisibility"),
            ],
            linkerSettings: [
                .linkedLibrary("c++")
            ]
        ),

        // Shared utilities
        .target(
            name: "CoreAIShared",
            dependencies: [],
            path: "swift/Sources/CoreAIShared",
            swiftSettings: [
                .enableUpcomingFeature("MemberImportVisibility")
            ]
        ),

        // CXGrammar C bridge
        .target(
            name: "CXGrammar",
            dependencies: [
                .product(name: "XGrammar", package: "xgrammar")
            ],
            path: "swift/Sources/lib/CXGrammar",
            publicHeadersPath: "include"
        ),
    ],
    swiftLanguageModes: [.v6],
    cxxLanguageStandard: .cxx17
)
