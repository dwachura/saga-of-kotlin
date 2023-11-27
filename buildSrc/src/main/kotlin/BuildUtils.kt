import org.gradle.accessors.dm.LibrariesForLibs
import org.gradle.api.Project
import org.gradle.kotlin.dsl.the

fun Project.artifactName(): String =
    when {
        this == rootProject || parent == null -> name
        else -> "${parent!!.artifactName()}-$name"
    }

fun Project.isSnapshot(): Boolean = "$version".endsWith("SNAPSHOT", ignoreCase = true)

internal val Project.libs
    get() = the<LibrariesForLibs>()
