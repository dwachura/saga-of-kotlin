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

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.7.0"
}
