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
    implementation(libs.slf4j)
    implementation(libs.kotlin.logging)

    testImplementation(libs.bundles.kotest)
    testRuntimeOnly(libs.logback)
}

tasks.test {
    useJUnitPlatform()
}
