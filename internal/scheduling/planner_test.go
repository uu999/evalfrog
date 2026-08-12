package scheduling

import (
	"fmt"
	"testing"
	"time"
)

func TestPlannerFairnessForOneTwoTenAndHundredProjects(t *testing.T) {
	for _, projectCount := range []int{1, 2, 10, 100} {
		projectCount := projectCount
		t.Run(fmt.Sprintf("projects-%d", projectCount), func(t *testing.T) {
			snapshot := fixtureSnapshot(projectCount, 20, ResourceBuiltin)
			plan, err := BuildPlan(snapshot, 128, projectCount*10, map[ResourceClass]int{ResourceBuiltin: projectCount * 10}, 1)
			if err != nil {
				t.Fatal(err)
			}
			counts := admissionCounts(plan)
			for index := 0; index < projectCount; index++ {
				projectID := fmt.Sprintf("project-%03d", index)
				if counts[projectID] != 10 {
					t.Fatalf("project=%s admissions=%d", projectID, counts[projectID])
				}
			}
		})
	}
}

func TestPlannerBorrowsIdleDemandAndRespectsExistingInflight(t *testing.T) {
	snapshot := AuthoritySnapshot{
		Candidates: append(
			fixtureCandidates("small", 2, ResourceBuiltin),
			fixtureCandidates("busy", 20, ResourceBuiltin)...,
		),
		Inflight: fixtureInflight("busy", 4, ResourceBuiltin),
	}
	plan, err := BuildPlan(snapshot, 8, 10, map[ResourceClass]int{ResourceBuiltin: 10}, 1)
	if err != nil {
		t.Fatal(err)
	}
	counts := admissionCounts(plan)
	if counts["small"] != 2 || counts["busy"] != 4 {
		t.Fatalf("admissions=%v", counts)
	}
}

func TestPlannerNewProjectRecoversFairShareWithoutPreemption(t *testing.T) {
	snapshot := AuthoritySnapshot{
		Candidates: append(
			fixtureCandidates("incumbent", 20, ResourceBuiltin),
			fixtureCandidates("newcomer", 20, ResourceBuiltin)...,
		),
		Inflight: fixtureInflight("incumbent", 6, ResourceBuiltin),
	}
	plan, err := BuildPlan(snapshot, 8, 10, map[ResourceClass]int{ResourceBuiltin: 10}, 1)
	if err != nil {
		t.Fatal(err)
	}
	counts := admissionCounts(plan)
	if counts["incumbent"] != 0 || counts["newcomer"] != 4 {
		t.Fatalf("admissions=%v", counts)
	}
	if len(snapshot.Inflight) != 6 {
		t.Fatal("planner preempted existing attempts")
	}
}

func TestPlannerRotatesTiesAcrossEpochsAndDoesNotStarve(t *testing.T) {
	snapshot := fixtureSnapshot(10, 5, ResourceBuiltin)
	counts := map[string]int{}
	for epoch := uint64(1); epoch <= 20; epoch++ {
		plan, err := BuildPlan(snapshot, 16, 3, map[ResourceClass]int{ResourceBuiltin: 3}, epoch)
		if err != nil {
			t.Fatal(err)
		}
		for projectID, value := range admissionCounts(plan) {
			counts[projectID] += value
		}
	}
	minimum, maximum := 1000, 0
	for index := 0; index < 10; index++ {
		value := counts[fmt.Sprintf("project-%03d", index)]
		minimum, maximum = min(minimum, value), max(maximum, value)
	}
	if minimum == 0 || maximum-minimum > 1 {
		t.Fatalf("rotating fairness=%v", counts)
	}
}

func TestPlannerHonorsPoolAndGlobalDispatchWindows(t *testing.T) {
	snapshot := AuthoritySnapshot{Candidates: append(
		fixtureCandidates("builtin-project", 20, ResourceBuiltin),
		fixtureCandidates("sandbox-project", 20, ResourceSandbox)...,
	)}
	plan, err := BuildPlan(snapshot, 8, 10, map[ResourceClass]int{ResourceBuiltin: 8, ResourceSandbox: 2}, 1)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[ResourceClass]int{}
	for _, lane := range plan.Lanes {
		for _, admission := range lane.Admissions {
			counts[admission.ResourceClass]++
		}
	}
	if plan.TotalAdmissions != 10 || counts[ResourceBuiltin] != 8 || counts[ResourceSandbox] != 2 {
		t.Fatalf("total=%d pools=%v", plan.TotalAdmissions, counts)
	}
}

func TestPlannerDeduplicatesInflightAndRejectsInvalidFacts(t *testing.T) {
	snapshot := fixtureSnapshot(1, 2, ResourceBuiltin)
	value := Inflight{AttemptID: "attempt", ProjectID: "project-000", ResourceClass: ResourceBuiltin}
	snapshot.Inflight = []Inflight{value, value}
	plan, err := BuildPlan(snapshot, 8, 2, map[ResourceClass]int{ResourceBuiltin: 2}, 1)
	if err != nil || plan.TotalAdmissions != 1 {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}
	if _, err = BuildPlan(AuthoritySnapshot{Candidates: []Candidate{{}}}, 8, 1, map[ResourceClass]int{}, 1); err == nil {
		t.Fatal("invalid candidate accepted")
	}
	if _, err = BuildPlan(AuthoritySnapshot{Inflight: []Inflight{{}}}, 8, 1, map[ResourceClass]int{}, 1); err == nil {
		t.Fatal("invalid inflight accepted")
	}
	if _, err = BuildPlan(AuthoritySnapshot{}, 3, -1, nil, 0); err == nil {
		t.Fatal("invalid planner dimensions accepted")
	}
}

func TestCandidateOrderingLaneStabilityAndCreditBatchBoundary(t *testing.T) {
	now := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	candidates := []Candidate{
		candidate("p", "node-b", 1, now, ResourceBuiltin),
		candidate("p", "node-low", 0, now.Add(-time.Hour), ResourceBuiltin),
		candidate("p", "node-a", 1, now, ResourceBuiltin),
		candidate("p", "node-old", 1, now.Add(-time.Second), ResourceBuiltin),
	}
	sortCandidates(candidates)
	want := []string{"node-old", "node-a", "node-b", "node-low"}
	for index, value := range want {
		if candidates[index].NodeRunID != value {
			t.Fatalf("order=%v", candidates)
		}
	}
	lane, err := LaneFor("stable-project", 128)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 100; index++ {
		if again, _ := LaneFor("stable-project", 128); again != lane {
			t.Fatal("stable hash changed")
		}
	}
	values := make([]PlannedAdmission, 17)
	batches, err := CreditBatches(values, 8)
	if err != nil || len(batches) != 3 || len(batches[0]) != 8 || len(batches[1]) != 8 || len(batches[2]) != 1 {
		t.Fatalf("batches=%v err=%v", batchLengths(batches), err)
	}
}

func TestCapacityWindowChangeIsBoundedPerEpoch(t *testing.T) {
	previous := Windows{Global: 100, Pools: map[ResourceClass]int{ResourceBuiltin: 80, ResourceSandbox: 20}}
	desired := Windows{Global: 200, Pools: map[ResourceClass]int{ResourceBuiltin: 160, ResourceSandbox: 40}}
	bounded, err := BoundWindows(previous, desired, 0.1)
	if err != nil || bounded.Global != 110 || bounded.Pools[ResourceBuiltin] != 88 || bounded.Pools[ResourceSandbox] != 22 {
		t.Fatalf("bounded=%+v err=%v", bounded, err)
	}
	decreased, err := BoundWindows(previous, Windows{Global: 10, Pools: map[ResourceClass]int{ResourceBuiltin: 8, ResourceSandbox: 2}}, 0.1)
	if err != nil || decreased.Global != 10 || decreased.Pools[ResourceBuiltin] != 8 || decreased.Pools[ResourceSandbox] != 2 {
		t.Fatalf("decreased=%+v err=%v", decreased, err)
	}
}

func TestStableHashLaneDistributionHasNoSevereHotspot(t *testing.T) {
	const lanes = 128
	counts := make([]int, lanes)
	for index := 0; index < 10000; index++ {
		lane, err := LaneFor(fmt.Sprintf("distribution-project-%05d", index), lanes)
		if err != nil {
			t.Fatal(err)
		}
		counts[lane]++
	}
	minimum, maximum := 10000, 0
	for _, count := range counts {
		minimum, maximum = min(minimum, count), max(maximum, count)
	}
	if minimum < 45 || maximum > 120 {
		t.Fatalf("lane distribution min=%d max=%d counts=%v", minimum, maximum, counts)
	}
}

func fixtureSnapshot(projects, demand int, class ResourceClass) AuthoritySnapshot {
	result := AuthoritySnapshot{}
	for index := 0; index < projects; index++ {
		result.Candidates = append(result.Candidates, fixtureCandidates(fmt.Sprintf("project-%03d", index), demand, class)...)
	}
	return result
}

func fixtureCandidates(project string, count int, class ResourceClass) []Candidate {
	now := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	result := make([]Candidate, count)
	for index := range result {
		result[index] = candidate(project, fmt.Sprintf("%s-node-%03d", project, index), 0, now.Add(time.Duration(index)*time.Millisecond), class)
	}
	return result
}

func candidate(project, node string, priority int, at time.Time, class ResourceClass) Candidate {
	return Candidate{ProjectID: project, RunID: project + "-run", NodeRunID: node, ExecutionNodeID: "xn_" + node, StateVersion: 1, Priority: priority, ReadyAt: at, ResourceClass: class}
}

func fixtureInflight(project string, count int, class ResourceClass) []Inflight {
	result := make([]Inflight, count)
	for index := range result {
		result[index] = Inflight{AttemptID: fmt.Sprintf("%s-attempt-%d", project, index), ProjectID: project, ResourceClass: class}
	}
	return result
}

func admissionCounts(plan Plan) map[string]int {
	result := map[string]int{}
	for _, lane := range plan.Lanes {
		for _, admission := range lane.Admissions {
			result[admission.ProjectID]++
		}
	}
	return result
}

func batchLengths(values [][]PlannedAdmission) []int {
	result := make([]int, len(values))
	for index := range values {
		result[index] = len(values[index])
	}
	return result
}
