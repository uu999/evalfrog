package scheduling

import (
	"fmt"
	"sort"
)

// BuildPlan performs progressive filling over project inflight counts. A
// project with insufficient demand releases its unused share immediately, so
// the result is equal-weight Max-Min and work-conserving.
func BuildPlan(snapshot AuthoritySnapshot, laneCount, globalWindow int, poolWindows map[ResourceClass]int, epoch uint64) (Plan, error) {
	if laneCount <= 0 || laneCount&(laneCount-1) != 0 || globalWindow < 0 || epoch == 0 {
		return Plan{}, fmt.Errorf("planner dimensions are invalid")
	}
	candidates := append([]Candidate(nil), snapshot.Candidates...)
	for _, candidate := range candidates {
		if err := candidate.Validate(); err != nil {
			return Plan{}, err
		}
	}
	sortCandidates(candidates)
	queues := make(map[string][]Candidate)
	projectInflight := make(map[string]int)
	poolInflight := make(map[ResourceClass]int)
	seenInflight := make(map[string]struct{})
	for _, candidate := range candidates {
		queues[candidate.ProjectID] = append(queues[candidate.ProjectID], candidate)
	}
	for _, value := range snapshot.Inflight {
		if value.AttemptID == "" || value.ProjectID == "" || !value.ResourceClass.Valid() {
			return Plan{}, fmt.Errorf("inflight reservation is invalid")
		}
		if _, duplicate := seenInflight[value.AttemptID]; duplicate {
			continue
		}
		seenInflight[value.AttemptID] = struct{}{}
		projectInflight[value.ProjectID]++
		poolInflight[value.ResourceClass]++
	}
	remainingGlobal := globalWindow - len(seenInflight)
	if remainingGlobal < 0 {
		remainingGlobal = 0
	}
	remainingPool := make(map[ResourceClass]int, len(poolWindows))
	for class, window := range poolWindows {
		remainingPool[class] = max(0, window-poolInflight[class])
	}
	projects := make([]string, 0, len(queues))
	for projectID := range queues {
		projects = append(projects, projectID)
	}
	sort.Strings(projects)
	if len(projects) != 0 {
		offset := int((epoch - 1) % uint64(len(projects)))
		projects = append(append([]string(nil), projects[offset:]...), projects[:offset]...)
	}
	granted := make(map[string]int)
	planned := make(map[int][]PlannedAdmission)
	for remainingGlobal > 0 {
		selected := ""
		selectedLevel := 0
		for _, projectID := range projects {
			queue := queues[projectID]
			if len(queue) == 0 || remainingPool[queue[0].ResourceClass] == 0 {
				continue
			}
			level := projectInflight[projectID] + granted[projectID]
			if selected == "" || level < selectedLevel {
				selected, selectedLevel = projectID, level
			}
		}
		if selected == "" {
			break
		}
		candidate := queues[selected][0]
		queues[selected] = queues[selected][1:]
		lane, _ := LaneFor(selected, laneCount)
		planned[lane] = append(planned[lane], PlannedAdmission{ProjectID: selected, ResourceClass: candidate.ResourceClass})
		granted[selected]++
		remainingPool[candidate.ResourceClass]--
		remainingGlobal--
	}
	plan := Plan{Epoch: epoch, GlobalWindow: globalWindow, PoolWindows: clonePoolWindows(poolWindows), Lanes: make([]LanePlan, laneCount)}
	for lane := 0; lane < laneCount; lane++ {
		plan.Lanes[lane].Lane = lane
		plan.Lanes[lane].Admissions = planned[lane]
		plan.TotalAdmissions += len(planned[lane])
	}
	for _, candidate := range candidates {
		lane, _ := LaneFor(candidate.ProjectID, laneCount)
		plan.Lanes[lane].Candidates = append(plan.Lanes[lane].Candidates, candidate)
	}
	for _, value := range snapshot.Inflight {
		lane, laneErr := LaneFor(value.ProjectID, laneCount)
		if laneErr != nil {
			return Plan{}, laneErr
		}
		plan.Lanes[lane].Inflight = append(plan.Lanes[lane].Inflight, value)
	}
	return plan, nil
}

func CreditBatches(admissions []PlannedAdmission, size int) ([][]PlannedAdmission, error) {
	if size <= 0 {
		return nil, fmt.Errorf("credit batch size must be positive")
	}
	var result [][]PlannedAdmission
	for len(admissions) != 0 {
		count := min(size, len(admissions))
		result = append(result, append([]PlannedAdmission(nil), admissions[:count]...))
		admissions = admissions[count:]
	}
	return result, nil
}

func clonePoolWindows(values map[ResourceClass]int) map[ResourceClass]int {
	result := make(map[ResourceClass]int, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}
