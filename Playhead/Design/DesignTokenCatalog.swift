// DesignTokenCatalog.swift
// A developer preview surface that renders every design token so we can
// eyeball light/dark/AX sizes and so tests can render it through
// ImageRenderer. Not wired into production navigation.

import SwiftUI

struct DesignTokenCatalog: View {

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: Spacing.lg) {
                header("Colors")
                colorsSection

                header("Typography")
                typographySection

                header("Spacing")
                spacingSection

                header("Corner Radii")
                cornerRadiiSection

                header("Motion")
                motionSection

                header("Haptics")
                hapticsSection
            }
            .padding(Spacing.md)
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        .background(AppColors.background)
    }

    // MARK: Sections

    private var colorsSection: some View {
        VStack(alignment: .leading, spacing: Spacing.xs) {
            Group {
                swatchRow("ink", Palette.ink)
                swatchRow("charcoal", Palette.charcoal)
                swatchRow("bone", Palette.bone)
                swatchRow("copper", Palette.copper)
                swatchRow("mutedSage", Palette.mutedSage)
                swatchRow("softSteel", Palette.softSteel)
            }
            Divider().background(AppColors.textTertiary)
            Group {
                swatchRow("background",     AppColors.background)
                swatchRow("surface",        AppColors.surface)
                swatchRow("surfaceElevated", AppColors.surfaceElevated)
                swatchRow("textPrimary",    AppColors.textPrimary)
                swatchRow("textSecondary",  AppColors.textSecondary)
                swatchRow("textTertiary",   AppColors.textTertiary)
                swatchRow("accent",         AppColors.accent)
                swatchRow("accentSubtle",   AppColors.accentSubtle)
            }
        }
    }

    private var typographySection: some View {
        VStack(alignment: .leading, spacing: Spacing.sm) {
            Text("Title 28").font(AppTypography.title).foregroundStyle(AppColors.textPrimary)
            Text("Headline 20").font(AppTypography.headline).foregroundStyle(AppColors.textPrimary)
            Text("Body 17").font(AppTypography.body).foregroundStyle(AppColors.textPrimary)
            Text("Caption 13").font(AppTypography.caption).foregroundStyle(AppColors.textSecondary)
            Text("01:23:45").font(AppTypography.timestamp).foregroundStyle(AppColors.textSecondary)
            Text("Transcript serif body.")
                .font(AppTypography.transcript)
                .foregroundStyle(AppColors.textPrimary)
            Text("Transcript caption serif.")
                .font(AppTypography.transcriptCaption)
                .foregroundStyle(AppColors.textSecondary)
        }
    }

    private var spacingSection: some View {
        VStack(alignment: .leading, spacing: Spacing.xs) {
            spacingBar("xxs 4",  Spacing.xxs)
            spacingBar("xs 8",   Spacing.xs)
            spacingBar("sm 12",  Spacing.sm)
            spacingBar("md 16",  Spacing.md)
            spacingBar("lg 24",  Spacing.lg)
            spacingBar("xl 32",  Spacing.xl)
            spacingBar("xxl 48", Spacing.xxl)
        }
    }

    private var cornerRadiiSection: some View {
        HStack(spacing: Spacing.sm) {
            radiusBox("small",  CornerRadius.small)
            radiusBox("medium", CornerRadius.medium)
            radiusBox("large",  CornerRadius.large)
        }
    }

    private var motionSection: some View {
        VStack(alignment: .leading, spacing: Spacing.xs) {
            motionRow(Motion.quickDescriptor)
            motionRow(Motion.standardDescriptor)
            motionRow(Motion.deliberateDescriptor)
            motionRow(Motion.preciseEaseDescriptor)
            motionRow(Motion.transportDescriptor)
        }
    }

    private var hapticsSection: some View {
        VStack(alignment: .leading, spacing: Spacing.xxs) {
            Text("skip → medium impact")
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textSecondary)
            Text("control → light impact")
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textSecondary)
            Text("save → success notification")
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textSecondary)
        }
    }

    // MARK: Helpers

    private func header(_ text: String) -> some View {
        Text(text)
            .font(AppTypography.headline)
            .foregroundStyle(AppColors.textPrimary)
            .padding(.top, Spacing.sm)
    }

    private func swatchRow(_ name: String, _ color: Color) -> some View {
        HStack(spacing: Spacing.sm) {
            RoundedRectangle(cornerRadius: CornerRadius.small)
                .fill(color)
                .frame(width: 32, height: 32)
                .overlay(
                    RoundedRectangle(cornerRadius: CornerRadius.small)
                        .stroke(AppColors.textTertiary, lineWidth: 0.5)
                )
            Text(name)
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textSecondary)
        }
    }

    private func spacingBar(_ label: String, _ size: CGFloat) -> some View {
        HStack(spacing: Spacing.xs) {
            Rectangle().fill(AppColors.accent).frame(width: size, height: 14)
            Text(label)
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textSecondary)
        }
    }

    private func radiusBox(_ label: String, _ radius: CGFloat) -> some View {
        VStack(spacing: Spacing.xxs) {
            RoundedRectangle(cornerRadius: radius)
                .fill(AppColors.surface)
                .frame(width: 56, height: 40)
                .overlay(
                    RoundedRectangle(cornerRadius: radius)
                        .stroke(AppColors.textTertiary, lineWidth: 1)
                )
            Text(label)
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textSecondary)
        }
    }

    private func motionRow(_ descriptor: MotionDescriptor) -> some View {
        HStack {
            Text(descriptor.name)
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textPrimary)
            Spacer(minLength: Spacing.sm)
            Text("\(descriptor.kind.rawValue) \(String(format: "%.2fs", descriptor.duration))")
                .font(AppTypography.timestamp)
                .foregroundStyle(AppColors.textSecondary)
        }
    }
}

#Preview("Design Token Catalog — Dark") {
    DesignTokenCatalog()
        .preferredColorScheme(.dark)
}

#Preview("Design Token Catalog — Light") {
    DesignTokenCatalog()
        .preferredColorScheme(.light)
}
