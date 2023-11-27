plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    implementation(libs.gradle.plugins.kotlin)
    implementation(libs.gradle.plugins.testlogger)
    implementation(files(libs.javaClass.superclass.protectionDomain.codeSource.location))
}
