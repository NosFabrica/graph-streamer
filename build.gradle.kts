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
}

dependencies {
    implementation("com.vitorpamplona.quartz:quartz:2.14.1")
    // Quartz ships these as runtime-only; we compile against them.
    implementation("com.squareup.okhttp3:okhttp:5.5.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")

    implementation("org.neo4j.driver:neo4j-java-driver:5.28.15")
    implementation("redis.clients:jedis:7.2.0")
    implementation("org.slf4j:slf4j-simple:2.0.13")

    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.11.0")
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass.set("com.nosfabrica.graphstreamer.MainKt")
}

tasks.test {
    useJUnitPlatform()
}
