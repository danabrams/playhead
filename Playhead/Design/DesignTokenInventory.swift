// DesignTokenInventory.swift
// Enumerates every design token so tests can assert invariants across
// the whole token set (e.g. no purple/gradient/shimmer/spring keywords
// anywhere in the design system). This is the single source of truth
// for "what tokens exist".

import Foundation

enum DesignTokenCategory: String, Equatable, CaseIterable {
    case color
    case spacing
    case cornerRadius
    case motion
    case typography
    case haptic
}

struct DesignTokenEntry: Equatable {
    let name: String
    let category: DesignTokenCategory
}

enum DesignTokenInventory {
    static let all: [DesignTokenEntry] = colors + spacing + cornerRadii + motion + typography + haptics

    static let colors: [DesignTokenEntry] = [
        // Raw palette
        .init(name: "ink",            category: .color),
        .init(name: "charcoal",       category: .color),
        .init(name: "bone",           category: .color),
        .init(name: "copper",         category: .color),
        .init(name: "mutedSage",      category: .color),
        .init(name: "softSteel",      category: .color),
        // Semantic
        .init(name: "background",     category: .color),
        .init(name: "surface",        category: .color),
        .init(name: "surfaceElevated", category: .color),
        .init(name: "textPrimary",    category: .color),
        .init(name: "textSecondary",  category: .color),
        .init(name: "textTertiary",   category: .color),
        .init(name: "accent",         category: .color),
        .init(name: "accentSubtle",   category: .color)
    ]

    static let spacing: [DesignTokenEntry] = [
        .init(name: "xxs", category: .spacing),
        .init(name: "xs",  category: .spacing),
        .init(name: "sm",  category: .spacing),
        .init(name: "md",  category: .spacing),
        .init(name: "lg",  category: .spacing),
        .init(name: "xl",  category: .spacing),
        .init(name: "xxl", category: .spacing)
    ]

    static let cornerRadii: [DesignTokenEntry] = [
        .init(name: "small",  category: .cornerRadius),
        .init(name: "medium", category: .cornerRadius),
        .init(name: "large",  category: .cornerRadius)
    ]

    static let motion: [DesignTokenEntry] = [
        .init(name: "quick",       category: .motion),
        .init(name: "standard",    category: .motion),
        .init(name: "deliberate",  category: .motion),
        .init(name: "preciseEase", category: .motion),
        .init(name: "transport",   category: .motion)
    ]

    static let typography: [DesignTokenEntry] = [
        .init(name: "title",             category: .typography),
        .init(name: "headline",          category: .typography),
        .init(name: "body",              category: .typography),
        .init(name: "caption",           category: .typography),
        .init(name: "timestamp",         category: .typography),
        .init(name: "transcript",        category: .typography),
        .init(name: "transcriptCaption", category: .typography)
    ]

    static let haptics: [DesignTokenEntry] = [
        .init(name: "skip",    category: .haptic),
        .init(name: "control", category: .haptic),
        .init(name: "save",    category: .haptic)
    ]
}
