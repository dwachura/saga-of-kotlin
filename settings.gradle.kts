rootProject.name = "saga-of-kotlin"
gradle.rootProject {
    group = "io.dwsoft"
}

pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

include(
    ":core",
    ":coroutines",
)

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.7.0"
}

enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")
