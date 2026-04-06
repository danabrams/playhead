#!/usr/bin/env ruby
require "xcodeproj"

proj_path = "Playhead.xcodeproj"
proj = Xcodeproj::Project.open(proj_path)

app_target = proj.targets.find { |t| t.name == "Playhead" }
test_target = proj.targets.find { |t| t.name == "PlayheadTests" }

def find_group(parent, names)
  names.inject(parent) { |g, n| g.children.find { |c| c.display_name == n && c.respond_to?(:children) } }
end

def add_file(proj, group, target, abs_path)
  rel = abs_path.sub(File.expand_path("."), ".")
  fn = File.basename(abs_path)
  if group.children.any? { |c| c.display_name == fn }
    puts "skip (already in group): #{fn}"
    return
  end
  ref = group.new_reference(abs_path)
  ref.path = fn
  ref.source_tree = "<group>"
  target.add_file_references([ref])
  puts "added: #{fn} -> #{group.display_name} (#{target.name})"
end

ad_group_app = find_group(proj.main_group, ["Playhead", "Services", "AdDetection"])
ad_group_test = find_group(proj.main_group, ["PlayheadTests", "Services", "AdDetection"])
itests_group = find_group(proj.main_group, ["PlayheadTests", "IntegrationTests"])

helpers_group = find_group(proj.main_group, ["PlayheadTests", "Helpers"])
add_file(proj, helpers_group, test_target, File.expand_path("PlayheadTests/Helpers/TestFMRuntime.swift"))
add_file(proj, ad_group_app, app_target, File.expand_path("Playhead/Services/AdDetection/FMBackfillMode.swift"))
add_file(proj, ad_group_app, app_target, File.expand_path("Playhead/Services/AdDetection/BackfillJobRunner.swift"))
add_file(proj, ad_group_test, test_target, File.expand_path("PlayheadTests/Services/AdDetection/AdDetectionConfigTests.swift"))
add_file(proj, ad_group_test, test_target, File.expand_path("PlayheadTests/Services/AdDetection/BackfillJobRunnerTests.swift"))
add_file(proj, ad_group_test, test_target, File.expand_path("PlayheadTests/Services/AdDetection/AdDetectionServiceShadowModeTests.swift"))
add_file(proj, itests_group, test_target, File.expand_path("PlayheadTests/IntegrationTests/Phase3ShadowReplayHarnessTests.swift"))

proj.save
puts "saved."
