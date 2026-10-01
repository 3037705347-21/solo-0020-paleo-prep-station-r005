package repository

import (
	"sort"
	"sync"
	"sync/atomic"

	"example.com/paleo-prep-station/internal/domain"
)

type state struct {
	specimens map[string]domain.Specimen
	plans     map[string]domain.PrepPlan
	progress  map[string][]domain.ProgressEntry
	releases  map[string]domain.ReleaseRecord
}

type PlanSnapshot struct {
	Plan     domain.PrepPlan
	Progress []domain.ProgressEntry
}

// revision 记录缓存条目构建时对应的仓储版本，只有版本仍匹配的条目才能被读取。
type cachedPlanSnapshot struct {
	revision uint64
	value    PlanSnapshot
}

type cachedPlanList struct {
	revision uint64
	values   []domain.PrepPlan
}

type cachedSpecimenSnapshot struct {
	revision uint64
	value    domain.Specimen
}

type MemoryRepository struct {
	mu    sync.Mutex
	state state
	// revision 在每次 Mutate 提交时递增，用于识别提交前构建的缓存条目。
	revision atomic.Uint64

	viewMu            sync.RWMutex
	planSnapshots     map[string]cachedPlanSnapshot
	planLists         map[string]cachedPlanList
	specimenSnapshots map[string]cachedSpecimenSnapshot
}

func NewMemory() *MemoryRepository {
	return &MemoryRepository{
		state: state{
			specimens: make(map[string]domain.Specimen),
			plans:     make(map[string]domain.PrepPlan),
			progress:  make(map[string][]domain.ProgressEntry),
			releases:  make(map[string]domain.ReleaseRecord),
		},
		planSnapshots:     make(map[string]cachedPlanSnapshot),
		planLists:         make(map[string]cachedPlanList),
		specimenSnapshots: make(map[string]cachedSpecimenSnapshot),
	}
}

func (r *MemoryRepository) Read(fn func(*Mutation) error) error {
	_, err := r.ReadRevision(fn)
	return err
}

// ReadRevision 返回读取期间持有的仓储版本，调用方应把它传给对应的 Store 方法。
func (r *MemoryRepository) ReadRevision(fn func(*Mutation) error) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := fn(cloneMutation(r.state)); err != nil {
		return 0, err
	}
	return r.revision.Load(), nil
}

func (r *MemoryRepository) Mutate(fn func(*Mutation) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	tx := cloneMutation(r.state)
	if err := fn(tx); err != nil {
		return err
	}
	r.state = tx.snapshot()
	r.revision.Add(1)
	return nil
}

func (r *MemoryRepository) CachedPlanSnapshot(planID string) (PlanSnapshot, bool) {
	r.viewMu.RLock()
	entry, ok := r.planSnapshots[planID]
	r.viewMu.RUnlock()
	if !ok || entry.revision != r.revision.Load() {
		return PlanSnapshot{}, false
	}
	value := entry.value
	value.Progress = append([]domain.ProgressEntry(nil), value.Progress...)
	return value, true
}

func (r *MemoryRepository) StorePlanSnapshot(revision uint64, value PlanSnapshot) {
	if revision != r.revision.Load() {
		return
	}
	value.Progress = append([]domain.ProgressEntry(nil), value.Progress...)
	r.viewMu.Lock()
	defer r.viewMu.Unlock()
	r.planSnapshots[value.Plan.ID] = cachedPlanSnapshot{revision: revision, value: value}
}

func (r *MemoryRepository) CachedPlanList(specimenID string) ([]domain.PrepPlan, bool) {
	r.viewMu.RLock()
	entry, ok := r.planLists[specimenID]
	r.viewMu.RUnlock()
	if !ok || entry.revision != r.revision.Load() {
		return nil, false
	}
	return append([]domain.PrepPlan(nil), entry.values...), true
}

func (r *MemoryRepository) StorePlanList(revision uint64, specimenID string, values []domain.PrepPlan) {
	if revision != r.revision.Load() {
		return
	}
	r.viewMu.Lock()
	defer r.viewMu.Unlock()
	r.planLists[specimenID] = cachedPlanList{
		revision: revision,
		values:   append([]domain.PrepPlan(nil), values...),
	}
}

func (r *MemoryRepository) CachedSpecimenSnapshot(specimenID string) (domain.Specimen, bool) {
	r.viewMu.RLock()
	entry, ok := r.specimenSnapshots[specimenID]
	r.viewMu.RUnlock()
	if !ok || entry.revision != r.revision.Load() {
		return domain.Specimen{}, false
	}
	return entry.value, true
}

func (r *MemoryRepository) StoreSpecimenSnapshot(revision uint64, value domain.Specimen) {
	if revision != r.revision.Load() {
		return
	}
	r.viewMu.Lock()
	defer r.viewMu.Unlock()
	r.specimenSnapshots[value.ID] = cachedSpecimenSnapshot{revision: revision, value: value}
}

func (r *MemoryRepository) InvalidatePlanSnapshot(planID string) {
	r.viewMu.Lock()
	defer r.viewMu.Unlock()
	delete(r.planSnapshots, planID)
}

func (r *MemoryRepository) InvalidatePlanList(specimenID string) {
	r.viewMu.Lock()
	defer r.viewMu.Unlock()
	delete(r.planLists, specimenID)
}

func (r *MemoryRepository) InvalidateSpecimenSnapshot(specimenID string) {
	r.viewMu.Lock()
	defer r.viewMu.Unlock()
	delete(r.specimenSnapshots, specimenID)
}

func (r *MemoryRepository) InvalidatePreparationViews(specimenID, planID string) {
	r.viewMu.Lock()
	defer r.viewMu.Unlock()
	delete(r.planSnapshots, planID)
	delete(r.planLists, specimenID)
	delete(r.specimenSnapshots, specimenID)
}

func cloneMutation(source state) *Mutation {
	copied := state{
		specimens: make(map[string]domain.Specimen, len(source.specimens)),
		plans:     make(map[string]domain.PrepPlan, len(source.plans)),
		progress:  make(map[string][]domain.ProgressEntry, len(source.progress)),
		releases:  make(map[string]domain.ReleaseRecord, len(source.releases)),
	}
	for key, value := range source.specimens {
		copied.specimens[key] = value
	}
	for key, value := range source.plans {
		copied.plans[key] = value
	}
	for key, values := range source.progress {
		copied.progress[key] = append([]domain.ProgressEntry(nil), values...)
	}
	for key, value := range source.releases {
		copied.releases[key] = value
	}
	return &Mutation{state: copied}
}

type Mutation struct {
	state state
}

func (m *Mutation) snapshot() state {
	return m.state
}

func (m *Mutation) GetSpecimen(id string) (domain.Specimen, bool) {
	value, ok := m.state.specimens[id]
	return value, ok
}

func (m *Mutation) FindSpecimenByCatalog(catalogNumber string) (domain.Specimen, bool) {
	for _, value := range m.state.specimens {
		if value.CatalogNumber == catalogNumber {
			return value, true
		}
	}
	return domain.Specimen{}, false
}

func (m *Mutation) ListSpecimens() []domain.Specimen {
	values := make([]domain.Specimen, 0, len(m.state.specimens))
	for _, value := range m.state.specimens {
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].CreatedAt.Equal(values[j].CreatedAt) {
			return values[i].ID < values[j].ID
		}
		return values[i].CreatedAt.Before(values[j].CreatedAt)
	})
	return values
}

func (m *Mutation) PutSpecimen(value domain.Specimen) {
	m.state.specimens[value.ID] = value
}

func (m *Mutation) GetPlan(id string) (domain.PrepPlan, bool) {
	value, ok := m.state.plans[id]
	return value, ok
}

func (m *Mutation) ListPlansForSpecimen(specimenID string) []domain.PrepPlan {
	values := make([]domain.PrepPlan, 0)
	for _, value := range m.state.plans {
		if value.SpecimenID == specimenID {
			values = append(values, value)
		}
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].CreatedAt.Equal(values[j].CreatedAt) {
			return values[i].ID < values[j].ID
		}
		return values[i].CreatedAt.Before(values[j].CreatedAt)
	})
	return values
}

func (m *Mutation) PutPlan(value domain.PrepPlan) {
	m.state.plans[value.ID] = value
}

func (m *Mutation) HasOpenPlan(specimenID string) bool {
	for _, value := range m.state.plans {
		if value.SpecimenID == specimenID && value.IsOpen() {
			return true
		}
	}
	return false
}

func (m *Mutation) ProgressForPlan(planID string) []domain.ProgressEntry {
	return append([]domain.ProgressEntry(nil), m.state.progress[planID]...)
}

func (m *Mutation) AppendProgress(value domain.ProgressEntry) {
	m.state.progress[value.PlanID] = append(m.state.progress[value.PlanID], value)
}

func (m *Mutation) ReleaseForSpecimen(specimenID string) (domain.ReleaseRecord, bool) {
	for _, value := range m.state.releases {
		if value.SpecimenID == specimenID {
			return value, true
		}
	}
	return domain.ReleaseRecord{}, false
}

func (m *Mutation) PutRelease(value domain.ReleaseRecord) {
	m.state.releases[value.ID] = value
}
