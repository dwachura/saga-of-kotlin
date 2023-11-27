plugins {
    id("sok-conventions")
    id("sok-publishing-conventions")
}

dependencies {
    implementation(projects.core)
    implementation(libs.kotlinx.coroutines)
}
