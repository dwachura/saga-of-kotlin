plugins {
    kotlin("jvm")
    id("com.adarshr.test-logger")
//    signing apply false
//    id("org.jmailen.kotlinter")
}

version = rootProject.version

testlogger {
    showPassed = false
}

kotlin {
    jvmToolchain(11)
}

repositories {
    mavenLocal()
    mavenCentral()
    maven {
        url = uri(
            when {
                isSnapshot() -> "https://s01.oss.sonatype.org/content/repositories/snapshots/"
                else -> "https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/"
            }
        )
    }
}

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

dependencies {
    implementation(libs.slf4j)
    implementation(libs.kotlin.logging)

    testImplementation(libs.bundles.kotest)
    testRuntimeOnly(libs.logback)
}

tasks.test {
    useJUnitPlatform()
}
