pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
        maven("https://repo1.maven.org/maven2")
    }
}

rootProject.name = "graph-streamer"

// Build against a local Quartz checkout instead of the published artifact. Quartz's Android
// plugin needs Gradle 9.6+, which this wrapper (8.14) isn't, so run it with Quartz's wrapper and heap:
//   ../quartz/gradlew -p . -Dorg.gradle.jvmargs=-Xmx6g -PquartzDir=../quartz test
// For trying a Quartz change here before it is released. The dependency in build.gradle.kts still
// names the release, so a change that needs the new Quartz waits for that release to be committed.
providers.gradleProperty("quartzDir").orNull?.let { dir ->
    includeBuild(dir) {
        dependencySubstitution {
            substitute(module("com.github.vitorpamplona.quartz:quartz-jvm")).using(project(":quartz"))
        }
    }
}
