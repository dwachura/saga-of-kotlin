plugins {
    `java-library`
    `maven-publish`
//    signing apply false
}

val artifactName = artifactName()

tasks.jar {
    archiveBaseName.set(artifactName)
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "${rootProject.group}"
            artifactId = artifactName
            version = "${rootProject.version}"
            from(components["java"])
        }
    }

    repositories {
        maven {
            url = uri(
                when {
                    isSnapshot() -> "https://s01.oss.sonatype.org/content/repositories/snapshots/"
                    else -> "https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/"
                }
            )
            credentials {
                username = ""
                password = ""
            }
        }
    }
}

//signing {
//    sign(publishing.publications["maven"])
//}
