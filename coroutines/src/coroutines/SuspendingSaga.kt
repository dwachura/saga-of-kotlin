package io.dwsoft.sok.coroutines

import io.dwsoft.sok.SagaBuildingScope
import io.dwsoft.sok.SagaResult
import kotlinx.coroutines.CoroutineScope

fun saga(
    coroutineScope: CoroutineScope,
    builder: context (CoroutineScope) SagaBuildingScope<Any, Any>.() -> SagaResult<Any>
): Nothing = TODO()
