plugins {
    // Quartz 2.13.0 is built with Kotlin 2.4.21; an older compiler can't read its metadata.
    kotlin("jvm") version "2.4.21"
    application
}

group = "com.nosfabrica"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven("https://repo1.maven.org/maven2")
    // Quartz depends on AndroidX KMP artifacts (sqlite, collection), published there.
    google()
    // Quartz 2.16.0 isn't on Maven Central: the vitorpamplona org hit Central's monthly release
    // size limit (it resets 2026-12-01). JitPack built the v2.16.0 tag; only Quartz comes from it.
    maven("https://jitpack.io") {
        content { includeGroup("com.github.vitorpamplona.quartz") }
    }
}

dependencies {
    // JitPack's root `quartz` module names a different group from its path, so depend on the JVM
    // variant directly. Back to com.vitorpamplona.quartz:quartz once Central has a release.
    implementation("com.github.vitorpamplona.quartz:quartz-jvm:2.16.0")
    // Quartz ships these as runtime-only; we compile against them.
    implementation("com.squareup.okhttp3:okhttp:5.5.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")

    implementation("org.neo4j.driver:neo4j-java-driver:5.28.15")
    implementation("redis.clients:jedis:7.2.0")
    implementation("org.slf4j:slf4j-simple:2.0.13")

    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.11.0")
    // The end-to-end suite's relay: Quartz's NostrServer over its SQLite store, served by Ktor.
    testImplementation("io.ktor:ktor-server-cio:3.6.1")
    testImplementation("io.ktor:ktor-server-websockets:3.6.1")
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass.set("com.nosfabrica.graphstreamer.MainKt")
}

tasks.test {
    useJUnitPlatform()
    exclude("**/e2e/**")
}

/**
 * The end-to-end suite (src/test/.../e2e): the installed service against a local relay, Neo4j
 * and Redis, checked by tools/verify. Needs Docker containers (see E2E.md); not part of `test`.
 */
tasks.register<Test>("e2eTest") {
    description = "End-to-end scenarios against a local relay, Neo4j and Redis."
    group = "verification"
    useJUnitPlatform()
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    include("**/e2e/**")
    dependsOn("installDist")
    systemProperty("graphstreamer.bin", layout.buildDirectory.file("install/graph-streamer/bin/graph-streamer").get().asFile.absolutePath)
    systemProperty("graphstreamer.otherWriters", layout.projectDirectory.file("tools/rehearsal/other_writers.py").asFile.absolutePath)
    systemProperty("graphstreamer.serverBuild", layout.projectDirectory.file("tools/rehearsal/server_build.py").asFile.absolutePath)
    systemProperty("graphstreamer.verify", layout.projectDirectory.file("tools/verify/verify_graph.py").asFile.absolutePath)
    maxParallelForks = 1
    testLogging { events("passed", "failed", "skipped"); showStandardStreams = true; exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL }
}
