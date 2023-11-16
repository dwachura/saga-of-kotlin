plugins {
    alias(libs.plugins.kotlin.jvm)
}

group = rootProject.group
version = "1.0-SNAPSHOT"

kotlin {
    jvmToolchain(11)
    sourceSets {
        main.configure {
            kotlin.srcDirs("src")
            resources.srcDirs("resources")
        }
        test.configure {
            kotlin.srcDirs("test/src")
            resources.srcDirs("test/resources")
        }
    }
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(libs.bundles.kotest)
}

tasks.test {
    useJUnitPlatform()
}
