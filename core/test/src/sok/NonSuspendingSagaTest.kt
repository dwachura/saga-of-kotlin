package io.dwsoft.sok

import io.dwsoft.sok.Controlled.Context.Async
import io.github.oshai.kotlinlogging.KotlinLogging
import io.kotest.assertions.asClue
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.assertions.throwables.shouldThrowAny
import io.kotest.core.spec.style.FreeSpec
import io.kotest.core.spec.style.scopes.ContainerScope
import io.kotest.inspectors.forAll
import io.kotest.matchers.collections.shouldBeIn
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import io.mockk.verifyAll
import io.mockk.verifySequence
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.asCompletableFuture
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runCurrent
import java.util.concurrent.CompletableFuture.supplyAsync
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.time.Duration.Companion.ZERO
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

// TODO:
//  - replace Controlled saga with real one for tests of successful, failure and fatal results,
//  action's result access, etc. (saga's basic behavior stuff)
//
//  maybe group test cases by execution and rollback phases
/*
 * TODO:
 *  - test for compensation of actions that finished just before being cancelled
 *  - test for not compensating actions that failed just before being cancelled
 *  - test for not compensating actions that failed just before being cancelled
 */
class NonSuspendingSagaTest : FreeSpec({
    "Completed saga returns successful result" {
        val expectedResult = (1..4).toList()
        val saga = saga<List<Int>, Nothing> {
            val v1 = atomic({ expectedResult[0] }, compensateWith = {})
            val v2 = atomic(asynchronous({ supplyAsync { expectedResult[1] }.asAsync() }, compensateWith = {}))
            val v3 = atomic(saga {
                val v31 = atomic({ expectedResult[2] }, compensateWith = {})
                val v32 = atomic(asynchronous({ supplyAsync { expectedResult[3] }.asAsync() }, compensateWith = {}))
                result { listOf(v31, v32) }
            })
            result {
                listOf(v1, v2, *v3.value.toTypedArray()).map { it.value }
            }
        }

        val result = saga.execute()

        result.shouldBeSuccess() shouldBe expectedResult
    }

    "Saga can fail from" - {
        "$name non-asynchronous actions" {
            val result = saga<Unit, String> {
                atomic({ fail("Test") }, compensateWith = {})
                result {}
            }.execute()

            result.shouldBeFailure() shouldBe "Test"
        }

        "$name asynchronous actions" {
            val result = saga<Unit, String> {
                atomic(asynchronous({ supplyAsync { fail("Test") }.asAsync() }, compensateWith = {}))
                result {}
            }.execute()

            result.shouldBeFailure() shouldBe "Test"
        }

        "$name completion blocks" {
            val result = saga<Unit, String> {
                result { fail("Test") }
            }.execute()

            result.shouldBeFailure() shouldBe "Test"
        }
    }

    "Saga throws exception when exception is thrown from" - {
        "$name non-asynchronous action" {
            val expectedCause = RuntimeException()
            val saga = saga<Unit, String> {
                atomic({ throw expectedCause }, compensateWith = {})
                result {}
            }

            shouldThrow<SagaException> {
                saga.execute()
            }.cause shouldBe expectedCause
        }

        "$name asynchronous action" {
            val expectedCause = RuntimeException()
            val saga = saga<Unit, String> {
                atomic(asynchronous({ supplyAsync { throw expectedCause }.asAsync() }, compensateWith = {}))
                result {}
            }

            shouldThrow<SagaException> {
                saga.execute()
            }.cause shouldBe expectedCause
        }

        "$name completion" {
            val expectedCause = RuntimeException()
            val saga = saga<Unit, String> {
                result { throw expectedCause }
            }

            shouldThrow<SagaException> {
                saga.execute()
            }.cause shouldBe expectedCause
        }

        "$name rollback" {
            val expectedCause = RuntimeException()
            val saga = saga<Unit, Unit> {
                atomic({}, compensateWith = { throw expectedCause })
                atomic({ fail(Unit) }, compensateWith = {})
                result {}
            }

            shouldThrow<SagaException> {
                saga.execute()
            }.cause shouldBe expectedCause
        }
    }

    "Action results are usable in" - {
        "$name actions" {
            val recorder = Recorder<Int>()
            saga<Unit, Nothing> {
                val v = atomic({ 1 }, compensateWith = {})
                atomic({ recorder(v.value * 1) }, compensateWith = {})
                atomic(asynchronous({ supplyAsync { recorder(v.value * 2) }.asAsync() }, compensateWith = {}))
                result {}
            }.execute()

            recorder.shouldBeCalledWith(1, 2)
        }

        "$name completion blocks" {
            val recorder = Recorder<Int>()
            saga<Unit, Nothing> {
                val v = atomic({ 1 }, compensateWith = {})
                atomic(saga {
                    result { recorder(v.value * 1) }
                })
                result { recorder(v.value * 2) }
            }.execute()

            recorder.shouldBeCalledWith(1, 2)
        }

        "$name rollbacks" {
            val recorder = Recorder<Int>()
            saga<Unit, Unit> {
                val v = atomic({ 1 }, compensateWith = {})
                atomic({}, compensateWith = { recorder(v.value * 1) })
                atomic(asynchronous({ supplyAsync {}.asAsync() }, compensateWith = { recorder(v.value * 2) }))
                result { fail(Unit) }
            }.execute()

            recorder.shouldBeCalledWith(2, 1)
        }
    }

    "Saga execution flow" - {
        "Non-asynchronous actions run synchronously" {
            val saga = buildTestSaga {
                saga<Unit, Nothing> {
                    testAtomic(sleep = 1.seconds) { 1 }
                    testAtomic(sleep = 1.seconds) { 2 }
                    result {}
                }
            }

            val result = saga.execute()

            result.duration shouldBe 2.seconds
            result.recorder.shouldBeCalledWith(1, 2)
        }

        "Asynchronous actions run in parallel" {
            val saga = buildTestSaga {
                saga<Unit, Nothing> {
                    testAtomic(Async, sleep = 2.seconds) { 2 }
                    testAtomic(Async, sleep = 1.seconds) { 1 }
                    result {}
                }
            }

            val result = saga.execute()

            result.duration shouldBe 2.seconds
            result.recorder.shouldBeCalledWith(1, 2)
        }

        "Completions are called after all actions complete" {
            val saga = buildTestSaga {
                saga {
                    testAtomic(Async, sleep = 1.seconds) { 2 }
                    testAtomic { 1 }
                    result { recorder(3) }
                }
            }

            val result = saga.execute()

            result.recorder.shouldBeCalledWith(1, 2, 3)
        }

        "Nested sagas runs as non-asynchronous actions" {
            val saga = buildTestSaga {
                saga<Unit, Nothing> {
                    atomic(saga {
                        testAtomic(Async, sleep = 2.seconds) { 2 }
                        testAtomic(Async, sleep = 1.seconds) { 1 }
                        result {}
                    })
                    atomic(saga {
                        testAtomic(sleep = 1.seconds) { 3 }
                        testAtomic(sleep = 1.seconds) { 4 }
                        result {}
                    })
                    result {}
                }
            }

            saga.execute {
                advanceUntilAnyActionCompletes() shouldBe 1.seconds
                recorder.shouldBeCalledWith(1)
                advanceUntilAnyActionCompletes() shouldBe 2.seconds
                recorder.shouldBeCalledWith(1, 2)
                advanceUntilAnyActionCompletes() shouldBe 3.seconds
                recorder.shouldBeCalledWith(1, 2, 3)
                advanceUntilDone().second shouldBe 4.seconds
                recorder.shouldBeCalledWith(1, 2, 3, 4)
            }
        }

        "All actions are invoked in order they were declared" {
            val invokeRecorder = Recorder<Int>()
            val saga = buildTestSaga {
                saga {
                    testAtomic(onInvoke = { invokeRecorder(1) }) { 1 }
                    testAtomic(Async, sleep = 3.seconds, onInvoke = { invokeRecorder(2) }) { 4 }
                    atomic(saga {
                        testAtomic(onInvoke = { invokeRecorder(3) }) { 2 }
                        testAtomic(Async, sleep = 1.seconds, onInvoke = { invokeRecorder(4) }) { 3 }
                        result {}
                    })
                    result {}
                }
            }

            saga.execute {
                advanceBy(2.seconds)
                invokeRecorder.shouldBeCalledWith(1, 2, 3, 4)
                recorder.shouldBeCalledWith(1, 2, 3)
                advanceUntilDone().second shouldBe 3.seconds
                recorder.shouldBeCalledWith(1, 2, 3, 4)
            }
        }

        "Unsuccessful execution" - {
            "Actions not started are not invoked when" - {
                "$name non-asynchronous action" - {
                    "$name fails" {
                        val saga = buildTestSaga {
                            saga {
                                testAtomic { fail(Unit) }
                                testAtomic {}
                                result {}
                            }
                        }

                        saga.execute()
                            .recorder.shouldNotBeCalled()
                    }

                    "$name throws exception" {
                        val unexpectedRecorder = Recorder<Any>()
                        val saga = buildTestSaga {
                            saga {
                                testAtomic { throw RuntimeException() }
                                testAtomic(recorder = unexpectedRecorder) {}
                                result {}
                            }
                        }

                        shouldThrowAny { saga.execute() }

                        unexpectedRecorder.shouldNotBeCalled()
                    }
                }

                "$name asynchronous action" - {
                    "$name fails" {
                        val unexpectedRecorder = Recorder<Unit>()
                        val saga = buildTestSaga {
                            saga {
                                testAtomic(Async, sleep = 1.seconds) { fail(Unit) }
                                testAtomic(sleep = 2.seconds) {}
                                testAtomic(Async, recorder = unexpectedRecorder) {}
                                testAtomic(recorder = unexpectedRecorder) {}
                                result {}
                            }
                        }

                        saga.execute()

                        unexpectedRecorder.shouldNotBeCalled()
                    }

                    "$name throws exception" {
                        val unexpectedRecorder = Recorder<Unit>()
                        val saga = buildTestSaga {
                            saga {
                                testAtomic(Async, sleep = 1.seconds) { throw RuntimeException() }
                                testAtomic(sleep = 2.seconds) {}
                                testAtomic(Async, recorder = unexpectedRecorder) {}
                                testAtomic(recorder = unexpectedRecorder) {}
                                result {}
                            }
                        }

                        shouldThrowAny { saga.execute() }

                        unexpectedRecorder.shouldNotBeCalled()
                    }
                }
            }

            "Asynchronous actions in progress are cancelled when" - {
                "$name non-asynchronous action fails" {
                    val unexpectedRecorder = Recorder<Any>()
                    val saga = buildTestSaga {
                        saga {
                            testAtomic(Async, spyAs = 1) { 1 }
                            testAtomic(Async, spyAs = 3, sleep = 2.seconds, recorder = unexpectedRecorder) {}
                            testAtomic(Async, spyAs = 2) { 2 }
                            testAtomic(Async, spyAs = 4, sleep = 2.seconds, recorder = unexpectedRecorder) {}
                            testAtomic(sleep = 1.seconds) { fail(Any()) }
                            result {}
                        }
                    }

                    val result = saga.execute()

                    result.value.shouldBeFailure()
                    result.duration shouldBe 1.seconds
                    result.recorder.shouldBeCalledWith(1, 2)
                    verify(exactly = 0) { result.asyncActionSpy(1).cancel() }
                    verify(exactly = 0) { result.asyncActionSpy(2).cancel() }
                    unexpectedRecorder.shouldNotBeCalled()
                    verifyAll {
                        result.asyncActionSpy(3).cancel()
                        result.asyncActionSpy(4).cancel()
                    }
                }

                "$name asynchronous action fails" {
                    val unexpectedRecorder = Recorder<Any>()
                    val saga = buildTestSaga {
                        saga {
                            testAtomic(Async, sleep = 1.seconds) { fail(Any()) }
                            testAtomic(Async, spyAs = 1) { 1 }
                            testAtomic(Async, spyAs = 3, sleep = 2.seconds, recorder = unexpectedRecorder) {}
                            testAtomic(Async, spyAs = 2) { 2 }
                            testAtomic(Async, spyAs = 4, sleep = 2.seconds, recorder = unexpectedRecorder) {}
                            result {}
                        }
                    }

                    val result = saga.execute()

                    result.value.shouldBeFailure()
                    result.duration shouldBe 1.seconds
                    result.recorder.shouldBeCalledWith(1, 2)
                    verify(exactly = 0) { result.asyncActionSpy(1).cancel() }
                    verify(exactly = 0) { result.asyncActionSpy(2).cancel() }
                    unexpectedRecorder.shouldNotBeCalled()
                    verifyAll {
                        result.asyncActionSpy(3).cancel()
                        result.asyncActionSpy(4).cancel()
                    }
                }
            }
        }
    }

    "Saga compensation" - {
        "Completed actions are compensated in reverse order they are defined when" - {
            "$name non-asynchronous action fails" {
                val expectedReason = Any()
                val unexpected = RollbackRecorder<Any>()
                val saga = buildTestSaga {
                    saga {
                        testAtomic { 1 }
                        testAtomic(Async, sleep = 2.seconds) { 2 }
                        atomic(saga {
                            testAtomic(Async, sleep = 1.seconds) { 3 }
                            testAtomic { 4 }
                            result {}
                        })
                        testAtomic(Async, sleep = 4.seconds, rollbackRecorder = unexpected) {}
                        testAtomic(sleep = 3.seconds, rollbackRecorder = unexpected) { fail(expectedReason) }
                        testAtomic(rollbackRecorder = unexpected) {}
                        result {}
                    }
                }

                val result = saga.execute()

                result.value.shouldBeFailure() shouldBe expectedReason
                result.rollbackRecorder.shouldBeCalledWith(4, 3, 2, 1)
                unexpected.shouldNotBeCalled()
            }

            "$name asynchronous action fails" {
                val expectedReason = Any()
                val unexpected = RollbackRecorder<Any>()
                val saga = buildTestSaga {
                    saga {
                        testAtomic(Async, sleep = 3.seconds, rollbackRecorder = unexpected) { fail(expectedReason) }
                        testAtomic { 1 }
                        testAtomic(Async, sleep = 2.seconds) { 2 }
                        atomic(saga {
                            testAtomic(Async, sleep = 1.seconds) { 3 }
                            testAtomic { 4 }
                            result {}
                        })
                        testAtomic(Async, sleep = 5.seconds, rollbackRecorder = unexpected) {}
                        testAtomic(sleep = 4.seconds) { 5 }
                        testAtomic(rollbackRecorder = unexpected) {}
                        result {}
                    }
                }

                val result = saga.execute()

                result.value.shouldBeFailure() shouldBe expectedReason
                result.rollbackRecorder.shouldBeCalledWith(5, 4, 3, 2, 1)
                unexpected.shouldNotBeCalled()
            }

            "$name completion fails" {
                val expectedReason = Any()
                val saga = buildTestSaga {
                    saga {
                        testAtomic { 1 }
                        testAtomic(Async, sleep = 2.seconds) { 2 }
                        atomic(saga {
                            testAtomic(Async, sleep = 1.seconds) { 3 }
                            testAtomic { 4 }
                            result {}
                        })
                        result { fail(expectedReason) }
                    }
                }

                val result = saga.execute()

                result.value.shouldBeFailure() shouldBe expectedReason
                result.rollbackRecorder.shouldBeCalledWith(4, 3, 2, 1)
            }
        }

        "Saga is not compensated when exception is thrown from" - {
            "$name non-asynchronous actions" {
                val unexpectedRecorder = RollbackRecorder<Int>()
                val saga = buildTestSaga {
                    saga {
                        testAtomic(rollbackRecorder = unexpectedRecorder) { 1 }
                        testAtomic(rollbackRecorder = unexpectedRecorder) { 2 }
                        testAtomic { throw RuntimeException() }
                        result {}
                    }
                }

                shouldThrow<SagaException> {
                    saga.execute()
                }
                unexpectedRecorder.shouldNotBeCalled()
            }

            "$name asynchronous actions" {
                val unexpectedRecorder = RollbackRecorder<Int>()
                val saga = buildTestSaga {
                    saga {
                        testAtomic(rollbackRecorder = unexpectedRecorder) { 1 }
                        testAtomic(rollbackRecorder = unexpectedRecorder) { 2 }
                        testAtomic(Async) { throw RuntimeException() }
                        result {}
                    }
                }

                shouldThrow<SagaException> {
                    saga.execute()
                }
                unexpectedRecorder.shouldNotBeCalled()
            }

            "$name completions" {
                val unexpectedRecorder = RollbackRecorder<Int>()
                val saga = buildTestSaga {
                    saga {
                        testAtomic(rollbackRecorder = unexpectedRecorder) { 1 }
                        testAtomic(rollbackRecorder = unexpectedRecorder) { 2 }
                        result { throw RuntimeException() }
                    }
                }

                shouldThrow<SagaException> {
                    saga.execute()
                }
                unexpectedRecorder.shouldNotBeCalled()
            }
        }

        "Cancelled asynchronous actions are not compensated" {
            TODO()
        }
    }

    // TODO: REFACTOR below

    "Disabled".config(enabled = false) - {
        "Parallel actions" - {
            "Compensation" - {
                "Completed actions are compensated" {
                    val rollback = RollbackRecorder<Int>()

                    val result = saga<Int, String> {
                        val v1 = atomic(asynchronous({ delayed(0.12.seconds) { 2 }.asAsync() }, compensateWith = rollback))
                        val v2 = atomic(asynchronous({ delayed(0.1.seconds) { 1 }.asAsync() }, compensateWith = rollback))
                        val v3 = atomic(
                            asynchronous(
                                { delayed(0.15.seconds) { fail("Test") }.asAsync() },
                                compensateWith = rollback
                            )
                        )
                        result { listOf(v1.value, v2.value, v3.value).sum() }
                    }.execute()

                    result.shouldBeInstanceOf<Saga.Result.Failure<String>>()
                        .reason shouldBe "Test"
                    rollback.shouldBeCalledWith(2, 1)
                }
            }

            "Only completely finished actions are compensated" {
                val rollback = RollbackRecorder<Int>()
                var cancelledFuture: Future<Int>? = null

                val result = saga<Int, String> {
                    val v1 = atomic(asynchronous({
                        delayed(0.1.seconds) { 1 }.asAsync()
                    }, compensateWith = rollback))
                    val v2 = atomic(asynchronous({
                        delayed(0.2.seconds) { 2 }.also { cancelledFuture = it }.asAsync()
                    }, compensateWith = rollback))
                    val v3 = atomic(asynchronous({
                        delayed(0.15.seconds) { fail("Test") }.asAsync()
                    }, compensateWith = rollback))
                    result { listOf(v1.value, v2.value, v3.value).sum() }
                }.execute()

                result.shouldBeInstanceOf<Saga.Result.Failure<String>>()
                    .reason shouldBe "Test"
                rollback.shouldBeCalledWith(1)
                cancelledFuture.shouldNotBeNull().isCancelled shouldBe true
            }

            "Partially completed nested sagas are compensated" {
                val rollback = RollbackRecorder<Int>()
                val unexpectedRollback = RollbackRecorder<Int>()
                var cancelledFuture: Future<Int>? = null

                val result = saga<Int, String> {
                    atomic(asynchronous({ delayed(0.1.seconds) { 1 }.asAsync() }, compensateWith = rollback))
                    val v = atomic(
                        saga {
                            atomic(asynchronous({ delayed(0.2.seconds) { 2 }.asAsync() }, compensateWith = rollback))
                            atomic(asynchronous({ delayed(0.21.seconds) { 3 }.asAsync() }, compensateWith = rollback))
                            atomic(
                                asynchronous(
                                    { delayed(0.3.seconds) { 4 }.also { cancelledFuture = it }.asAsync() },
                                    compensateWith = unexpectedRollback
                                )
                            )
                            val v = atomic(
                                asynchronous(
                                    { delayed(0.22.seconds) { fail("Test") }.asAsync() },
                                    compensateWith = unexpectedRollback
                                )
                            )
                            result { v.value }
                        }
                    )
                    result { v.value }
                }.execute()

                result.shouldBeInstanceOf<Saga.Result.Failure<String>>()
                    .reason shouldBe "Test"
                rollback.shouldBeCalledWith(3, 2, 1)
                unexpectedRollback.shouldNotBeCalled()
                cancelledFuture.shouldNotBeNull().isCancelled shouldBe true
            }

            "Uncompleted actions are cancelled and saga fail fast when error is thrown from asynchronous action" {
                val expectedException = RuntimeException()
                val unexpectedRollback = RollbackRecorder<Int>()
                var cancelledFuture: Future<out Int>? = null
                val saga = saga<Int, String> {
                    atomic(
                        asynchronous(
                            { delayed(0.3.seconds) { 1 }.also { cancelledFuture = it }.asAsync() },
                            compensateWith = unexpectedRollback
                        )
                    )
                    atomic(asynchronous({ delayed(0.1.seconds) { 2 }.asAsync() }, compensateWith = unexpectedRollback))
                    atomic(
                        asynchronous(
                            { delayed(0.2.seconds) { throw expectedException }.asAsync() },
                            compensateWith = unexpectedRollback
                        )
                    )
                    result { delayed(0.3.seconds) { 1 }.get() }
                }

                shouldThrow<SagaException> {
                    saga.execute()
                }.cause shouldBe expectedException
                unexpectedRollback.shouldNotBeCalled()
                cancelledFuture.shouldNotBeNull().isCancelled shouldBe true
            }

            "Completed actions are not cancelled".config(enabled = false, invocations = 100) {
                class ActionResult(val failure: Any = Any()) {
                    var future: Future<Nothing>? = null
                }

                val unexpectedRollback = RollbackRecorder<Int>()
                val actionsData = listOf(ActionResult(), ActionResult())
                val saga = saga<Int, Any> {
                    atomic(
                        asynchronous(
                            {
                                delayed(0.01.seconds) { fail(actionsData[0].failure) }.also { actionsData[0].future = it }
                                    .asAsync()
                            },
                            compensateWith = unexpectedRollback
                        )
                    )
                    atomic(
                        asynchronous(
                            {
                                delayed(0.01.seconds) { fail(actionsData[1].failure) }.also { actionsData[1].future = it }
                                    .asAsync()
                            },
                            compensateWith = unexpectedRollback
                        )
                    )
                    result { delayed(0.02.seconds) { 1 }.get() }
                }

                val result = saga.execute()

                result.shouldBeInstanceOf<Saga.Result.Failure<Any>>()
                    .reason.shouldBeIn(actionsData.map { it.failure })
                actionsData.forAll { it.future.shouldNotBeNull().isCancelled shouldBe false }
                unexpectedRollback.shouldNotBeCalled()
            }
        }
    }
})

private open class Recorder<T : Any>(
    private val mock: (T) -> Unit,
    private val argType: KClass<T>
) : (T) -> Unit by mock {
    fun shouldBeCalledWith(vararg args: T) {
        verifySequence {
            args.forEach { mock.invoke(it) }
        }
    }

    fun shouldNotBeCalled() {
        "Function should not be called".asClue {
            verify(exactly = 0) { mock.invoke(any(argType)) }
        }
    }

    companion object {
        inline operator fun <reified T : Any> invoke(): Recorder<T> =
            Recorder(
                mock = mockk<(T) -> Unit> {
                    every { this@mockk.invoke(any<T>()) } returns Unit
                },
                argType = T::class
            )
    }
}

private open class RollbackRecorder<T : Any> private constructor(
    private val rollback: SagaActionRollbackScope.(T) -> Unit,
    mock: (T) -> Unit,
    argType: KClass<T>,
) : Recorder<T>(mock, argType), (SagaActionRollbackScope, T) -> Unit by rollback {
    companion object {
        inline operator fun <reified T : Any> invoke(): RollbackRecorder<T> {
            val mock = mockk<(T) -> Unit> {
                every { this@mockk.invoke(any<T>()) } returns Unit
            }
            return RollbackRecorder(rollback = { mock(it) }, mock = mock, argType = T::class)
        }
    }
}

private fun <T> delayed(
    sleep: Duration,
    preDelay: () -> Unit = {},
    f: () -> T
): Future<T> =
    supplyAsync {
        preDelay()
        Thread.sleep(sleep.inWholeMilliseconds)
        f()
    }

private infix fun Duration.shouldBeBetween(range: ClosedRange<Duration>) {
    "Duration $this should be in range $range".asClue {
        (this in range) shouldBe true
    }
}

private fun <Success, Failure : Any> buildTestSaga(
    builder: context(Controlled.Context) () -> Saga.NonSuspending<Success, Failure>
): Controlled<Success, Failure> =
    Controlled(builder)

private class Controlled<Success, Failure : Any>(
    private val builder: context(Context) () -> Saga.NonSuspending<Success, Failure>
) {
    suspend fun execute(
        f: suspend Execution<Success, Failure>.() -> Unit = {},
    ): Result<Success, Failure> =
        TestScope(UnconfinedTestDispatcher()).let { sagaScope ->
            val context = Context(sagaScope)
            val saga = builder(context)
            val sagaResult = supplyAsync { saga.execute() }
            delay(100)
            val executionResult = Execution(sagaResult, sagaScope, context)
                .apply { f() }
                .advanceUntilDone()
            Result(executionResult.first, executionResult.second, context)
        }

    data class Result<Success, Failure : Any>(
        val value: Saga.Result<Success, Failure>,
        val duration: Duration,
        val recorder: Recorder<Any>,
        val rollbackRecorder: RollbackRecorder<Any>,
        val spiedAsyncCalls: Map<Any, AsyncCall<*>>
    ) {
        constructor(
            result: Saga.Result<Success, Failure>,
            duration: Duration,
            context: Context,
        ) : this(result, duration, context.recorder, context.rollbackRecorder, context.spiedAsyncCalls)

        fun asyncActionSpy(id: Any): AsyncCall<*> = spiedAsyncCalls[id].shouldNotBeNull()
    }

    class Context(private val scope: TestScope) {
        val recorder: Recorder<Any> = Recorder()
        val rollbackRecorder: RollbackRecorder<Any> = RollbackRecorder()

        private val _spiedAsyncCalls: MutableMap<Any, AsyncCall<*>> = mutableMapOf()
        val spiedAsyncCalls: Map<Any, AsyncCall<*>>
            get() = _spiedAsyncCalls

        // Because we want to separate child jobs
        private val parentJob = SupervisorJob(scope.coroutineContext[Job])

        // TODO: encapsulate
        val actions = mutableListOf<Deferred<*>>()

        fun <T : Any, F : Any> SagaActionScope<T, F>.action(
            sleep: Duration = ZERO,
            recorder: Recorder<in T> = this@Context.recorder,
            f: SagaActionScope<T, F>.() -> T
        ): Future<T> =
            scope.async(parentJob) {
                if (sleep != ZERO) delay(sleep)
                f().also(recorder)
            }.also { actions += it }
                .asCompletableFuture()

        fun <T : Any, F : Any> SagaBuildingScope<*, F>.testAtomic(
            async: Async? = null,
            sleep: Duration = ZERO,
            recorder: Recorder<in T> = this@Context.recorder,
            rollbackRecorder: RollbackRecorder<in T> = this@Context.rollbackRecorder,
            onInvoke: () -> Unit = {},
            spyAs: Any? = null,
            compensateWith: SagaActionRollbackScope.(T) -> Unit = {},
            f: SagaActionScope<T, F>.() -> T,
        ): LateResult<T> {
            val rollbackFunction: SagaActionRollbackScope.(T) -> Unit = {
                rollbackRecorder(it)
                compensateWith(it)
            }
            return when (async) {
                null -> {
                    require(spyAs == null) { "spyAs must be null in case of non-async actions" }
                    atomic({
                        logger.info { "Invoking sync action" }
                        onInvoke()
                        action(sleep, recorder, f).get()
                    }, rollbackFunction)
                }
                else -> {
                    atomic(asynchronous({
                        logger.info { "Invoking async action" }
                        onInvoke()
                        val action = action(sleep, recorder, f).asAsync()
                        spyAs?.let { action.spyAs(spyAs) } ?: action
                    }, rollbackFunction))
                }
            }
        }

        private fun <T> AsyncCall<T>.spyAs(id: Any): AsyncCall<T> =
            spyk(this).also { _spiedAsyncCalls[id] = it }

        object Async
    }

    // FIXME: sometimes test hangs on waiting
    //  e.g. test "Asynchronous actions in progress are cancelled when saga fails because of non-asynchronous action"
    class Execution<Success, Failure : Any>(
        private val sagaResult: Future<Saga.Result<Success, Failure>>,
        private val scope: TestScope,
        private val context: Context,
    ) {
        val recorder: Recorder<Any> = context.recorder
        val rollbackRecorder: RollbackRecorder<Any> = context.rollbackRecorder
        private val actions = context.actions

        fun elapsedSinceStart(): Duration = scope.currentTime.milliseconds

        fun advanceBy(duration: Duration) {
            logger.info { "advanceBy" }
            scope.advanceTimeBy(duration)
            scope.runCurrent()
            Thread.sleep(100)
        }

        fun advanceUntilAnyActionCompletes(): Duration {
            var completed: Deferred<*>? = null
            while (completed == null) {
                scope.advanceTimeBy(1.milliseconds)
                scope.runCurrent()
                completed = actions.firstOrNull { it.isCompleted }
            }
            actions.remove(completed)
            Thread.sleep(100)
            return elapsedSinceStart()
        }

        fun advanceUntilDone(): Pair<Saga.Result<Success, Failure>, Duration> =
            advanceUntilDone(3)


        private tailrec fun advanceUntilDone(retries: Int): Pair<Saga.Result<Success, Failure>, Duration> {
            logger.info { "advanceUntilDone" }
            while (actions.any { it.isActive }) {
                advanceUntilAnyActionCompletes()
            }
            if (retries == 1) {
                return runCatching {
                    sagaResult.get(1, TimeUnit.SECONDS)
                }.fold(
                    onSuccess = { it },
                    onFailure = {
                        val cause = (it as? ExecutionException)?.cause ?: it
                        throw cause
                    }
                ) to elapsedSinceStart()
            }
            Thread.sleep(25)
            return advanceUntilDone(retries - 1)
        }
    }
}

private fun <T> Saga.Result<T, *>.shouldBeSuccess(): T =
    shouldBeInstanceOf<Saga.Result.Success<T>>().value

private fun <T : Any> Saga.Result<*, T>.shouldBeFailure(): T =
    shouldBeInstanceOf<Saga.Result.Failure<T>>().reason

// TODO: move somewhere else
private val ContainerScope.name: String
    get() = testCase.name.testName

private val logger = KotlinLogging.logger {}
