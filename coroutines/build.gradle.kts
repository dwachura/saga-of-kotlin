plugins {
    id("sok-conventions")
    id("sok-publishing-conventions")
}

dependencies {
    api(projects.core)
    implementation(libs.kotlinx.coroutines)
}
