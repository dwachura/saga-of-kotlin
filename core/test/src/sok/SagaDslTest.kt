package io.dwsoft.sok

import io.kotest.assertions.throwables.shouldThrowMessage
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.shouldBe
import java.util.UUID
import kotlin.random.Random

class SagaDslTest : FreeSpec({
    "working completed saga is constructed" {
        val (expectedNum1, expectedNum2) = listOf(Random.nextInt(), Random.nextInt())
        val saga = saga {
            val x = phase { expectedNum1 } revertedBy {}
            val y = phase { x.value + expectedNum2 } revertedBy {}
            returning { x.value + y.value }
        }

        saga.execute() shouldBe expectedNum1 + (expectedNum1 + expectedNum2)
    }

    "working reverted saga is constructed" {
        val expectedUuid = UUID.randomUUID()
        val expectedNumber = Random.nextInt()
        val expectedErrorMessage = "Failed $expectedUuid:$expectedNumber"
        val map = mutableMapOf<UUID, Int>()

        val saga = saga {
            val uuid = phase { expectedUuid } revertedBy {}
            val number = phase {
                map[uuid.value] = expectedNumber
                expectedNumber
            } revertedBy { map.remove(uuid.value) }
            phase { error("Failed ${uuid.value}:${number.value}") } revertedBy {}
            returning {}
        }

        shouldThrowMessage(expectedErrorMessage) {
            saga.execute()
        }
    }
})

private fun <T> Saga<T>.execute(): T = invoke(sequentialSagaInvoker())
