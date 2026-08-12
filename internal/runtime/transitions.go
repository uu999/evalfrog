package runtime

func validRunTransition(from, to RunState) bool {
	switch from {
	case RunPending:
		return to == RunRunning || to == RunCanceled
	case RunRunning:
		return to == RunSucceeded || to == RunFailed || to == RunCanceled || to == RunTimedOut
	default:
		return false
	}
}

func validNodeTransition(kind NodeKind, from, to NodeState) bool {
	if kind == NodeControl {
		return from == NodePending && (to == NodeSucceeded || to == NodeFailed || to == NodeSkipped || to == NodeCanceled)
	}
	switch from {
	case NodePending:
		return to == NodeReady || to == NodeFailed || to == NodeSkipped || to == NodeCanceled
	case NodeReady:
		return to == NodeQueued || to == NodeCanceled
	case NodeQueued:
		return to == NodeRunning || to == NodeCanceled
	case NodeRunning:
		return to == NodeRetryWait || to == NodeSucceeded || to == NodeFailed || to == NodeTimedOut || to == NodeCanceled
	case NodeRetryWait:
		return to == NodeReady || to == NodeCanceled
	default:
		return false
	}
}

func validAttemptTransition(from, to AttemptState) bool {
	switch from {
	case AttemptQueued:
		return to == AttemptRunning || to == AttemptCanceled
	case AttemptRunning:
		return to == AttemptSucceeded || to == AttemptFailed || to == AttemptTimedOut || to == AttemptCanceled || to == AttemptLost
	default:
		return false
	}
}
