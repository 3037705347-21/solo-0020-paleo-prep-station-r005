package repository

import (
	"errors"
	"testing"
	"time"

	"example.com/paleo-prep-station/internal/domain"
)

var testNow = time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)

func seedPlan(t *testing.T, repo *MemoryRepository, percent int) (domain.Specimen, domain.PrepPlan, domain.ProgressEntry) {
	t.Helper()
	specimen, err := domain.NewSpecimen(
		"s1", "CATALOG-1", "北坡-07", "大型脊椎动物", "粉砂岩", "", testNow,
	)
	if err != nil {
		t.Fatalf("new specimen: %v", err)
	}
	plan, err := domain.NewPrepPlan("p1", specimen, "暴露主要关节面", "手工显微剥离", "", 80, testNow)
	if err != nil {
		t.Fatalf("new plan: %v", err)
	}
	if err := plan.Approve("curator-lin", testNow); err != nil {
		t.Fatalf("approve plan: %v", err)
	}
	entry, err := domain.NewProgressEntry("g1", plan, nil, "矩阵剥离", percent, "", "preparator-chen", testNow)
	if err != nil {
		t.Fatalf("new progress: %v", err)
	}
	if err := repo.Mutate(func(m *Mutation) error {
		m.PutSpecimen(specimen)
		m.PutPlan(plan)
		m.AppendProgress(entry)
		return nil
	}); err != nil {
		t.Fatalf("seed mutate: %v", err)
	}
	return specimen, plan, entry
}

func readAndStorePlanSnapshot(t *testing.T, repo *MemoryRepository) PlanSnapshot {
	t.Helper()
	var snapshot PlanSnapshot
	revision, err := repo.ReadRevision(func(m *Mutation) error {
		plan, ok := m.GetPlan("p1")
		if !ok {
			return errors.New("plan missing")
		}
		snapshot = PlanSnapshot{Plan: plan, Progress: m.ProgressForPlan("p1")}
		return nil
	})
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	repo.StorePlanSnapshot(revision, snapshot)
	return snapshot
}

// 高频查看同一批方案时命中缓存，不应重新扫描全部进度。
func TestPlanSnapshotCacheServesWithoutRescan(t *testing.T) {
	repo := NewMemory()
	seedPlan(t, repo, 10)

	cached := readAndStorePlanSnapshot(t, repo)
	if len(cached.Progress) != 1 || cached.Progress[0].Percent != 10 {
		t.Fatalf("unexpected cached snapshot: %+v", cached.Progress)
	}

	// 在不产生新提交（revision 不变）的情况下直接改动底层状态。
	// 命中缓存的读取必须仍返回缓存内容，而不是重新扫描进度。
	repo.mu.Lock()
	repo.state.progress["p1"] = append(repo.state.progress["p1"], domain.ProgressEntry{ID: "ghost", PlanID: "p1", Percent: 99})
	repo.mu.Unlock()

	got, ok := repo.CachedPlanSnapshot("p1")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if len(got.Progress) != 1 {
		t.Fatalf("cache hit rescanned state: got %d entries, want 1", len(got.Progress))
	}
}

// 写入成功后旧 revision 的缓存立即失效，读取入口不能继续返回上一段比例。
func TestPlanSnapshotCacheInvalidatedAfterCommit(t *testing.T) {
	repo := NewMemory()
	_, plan, entry := seedPlan(t, repo, 10)
	readAndStorePlanSnapshot(t, repo)

	if err := repo.Mutate(func(m *Mutation) error {
		next, err := domain.NewProgressEntry("g2", plan, []domain.ProgressEntry{entry}, "表面加固", 35, "", "preparator-chen", testNow)
		if err != nil {
			return err
		}
		m.AppendProgress(next)
		return nil
	}); err != nil {
		t.Fatalf("append progress: %v", err)
	}

	if _, ok := repo.CachedPlanSnapshot("p1"); ok {
		t.Fatal("cache must be invalidated after a successful commit")
	}

	fresh := readAndStorePlanSnapshot(t, repo)
	if len(fresh.Progress) != 2 || fresh.Progress[1].Percent != 35 {
		t.Fatalf("fresh snapshot = %+v, want two entries ending at 35%%", fresh.Progress)
	}
	if got, ok := repo.CachedPlanSnapshot("p1"); !ok || len(got.Progress) != 2 {
		t.Fatal("expected cache hit after fresh read")
	}
}

// 失败的写入不应让可用缓存失效；成功写入才推进 revision。
func TestFailedMutateDoesNotBumpRevision(t *testing.T) {
	repo := NewMemory()
	seedPlan(t, repo, 10)
	readAndStorePlanSnapshot(t, repo)

	err := repo.Mutate(func(m *Mutation) error {
		m.PutSpecimen(domain.Specimen{ID: "temp"})
		return errors.New("boom")
	})
	if err == nil {
		t.Fatal("expected mutate error")
	}
	if _, ok := repo.CachedPlanSnapshot("p1"); !ok {
		t.Fatal("valid cache must survive a failed mutate")
	}
}

// 短暂的读取失败后不能保留旧状态：刷新失败时绝不写入缓存。
func TestFailedReadDoesNotServeStaleCache(t *testing.T) {
	repo := NewMemory()
	seedPlan(t, repo, 10)
	readAndStorePlanSnapshot(t, repo)

	// 一次成功提交让旧缓存过期。
	if err := repo.Mutate(func(m *Mutation) error { return nil }); err != nil {
		t.Fatalf("bump revision: %v", err)
	}
	if _, ok := repo.CachedPlanSnapshot("p1"); ok {
		t.Fatal("stale cache must not be served after commit")
	}

	revision, err := repo.ReadRevision(func(m *Mutation) error {
		return errors.New("temporary read failure")
	})
	if err == nil {
		t.Fatal("expected read error")
	}
	// 读取失败时按服务层约定不会调用 Store；即使误传了返回的 revision 也必须被拒绝。
	repo.StorePlanSnapshot(revision, PlanSnapshot{Plan: domain.PrepPlan{ID: "p1"}, Progress: nil})
	if cached, ok := repo.CachedPlanSnapshot("p1"); ok {
		t.Fatalf("failed read must not populate cache, got %+v", cached)
	}

	fresh := readAndStorePlanSnapshot(t, repo)
	if len(fresh.Progress) != 1 {
		t.Fatalf("fresh snapshot after recovery = %d entries, want 1", len(fresh.Progress))
	}
}

// 读取发生在提交前、Store 发生在提交后时，过期结果必须被拒绝，避免旧状态覆盖新缓存。
func TestStaleStoreRejectedAfterConcurrentCommit(t *testing.T) {
	repo := NewMemory()
	seedPlan(t, repo, 10)

	staleRevision, err := repo.ReadRevision(func(m *Mutation) error { return nil })
	if err != nil {
		t.Fatalf("read revision: %v", err)
	}
	if err := repo.Mutate(func(m *Mutation) error { return nil }); err != nil {
		t.Fatalf("bump revision: %v", err)
	}

	repo.StorePlanSnapshot(staleRevision, PlanSnapshot{
		Plan:     domain.PrepPlan{ID: "p1", Status: domain.PlanApproved},
		Progress: []domain.ProgressEntry{{ID: "stale"}},
	})
	if _, ok := repo.CachedPlanSnapshot("p1"); ok {
		t.Fatal("store built on an expired revision must be discarded")
	}
}

// 缓存返回的切片是副本，调用方修改不会污染后续命中。
func TestCachedSnapshotProgressIsCopied(t *testing.T) {
	repo := NewMemory()
	seedPlan(t, repo, 10)
	readAndStorePlanSnapshot(t, repo)

	first, ok := repo.CachedPlanSnapshot("p1")
	if !ok {
		t.Fatal("expected cache hit")
	}
	first.Progress[0].Percent = 77
	first.Progress = append(first.Progress, domain.ProgressEntry{ID: "tampered"})

	second, ok := repo.CachedPlanSnapshot("p1")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if len(second.Progress) != 1 || second.Progress[0].Percent != 10 {
		t.Fatalf("cache was mutated through a returned slice: %+v", second.Progress)
	}
}

// 方案列表与标本快照同样按 revision 失效，并在过期 Store 前保持保护。
func TestPlanListAndSpecimenTrackRevision(t *testing.T) {
	repo := NewMemory()
	specimen, _, _ := seedPlan(t, repo, 10)

	var values []domain.PrepPlan
	listRevision, err := repo.ReadRevision(func(m *Mutation) error {
		values = m.ListPlansForSpecimen("s1")
		return nil
	})
	if err != nil {
		t.Fatalf("read list: %v", err)
	}
	repo.StorePlanList(listRevision, "s1", values)

	specRevision, err := repo.ReadRevision(func(m *Mutation) error {
		found, ok := m.GetSpecimen("s1")
		if !ok {
			return errors.New("specimen missing")
		}
		specimen = found
		return nil
	})
	if err != nil {
		t.Fatalf("read specimen: %v", err)
	}
	repo.StoreSpecimenSnapshot(specRevision, specimen)

	if _, ok := repo.CachedPlanList("s1"); !ok {
		t.Fatal("expected plan list cache hit")
	}
	if _, ok := repo.CachedSpecimenSnapshot("s1"); !ok {
		t.Fatal("expected specimen cache hit")
	}

	if err := repo.Mutate(func(m *Mutation) error { return nil }); err != nil {
		t.Fatalf("bump revision: %v", err)
	}
	if _, ok := repo.CachedPlanList("s1"); ok {
		t.Fatal("plan list cache must be invalidated after commit")
	}
	if _, ok := repo.CachedSpecimenSnapshot("s1"); ok {
		t.Fatal("specimen cache must be invalidated after commit")
	}

	// 用旧 revision 回填必须全部拒绝。
	repo.StorePlanList(listRevision, "s1", values)
	repo.StoreSpecimenSnapshot(specRevision, specimen)
	if _, ok := repo.CachedPlanList("s1"); ok {
		t.Fatal("stale plan list store must be rejected")
	}
	if _, ok := repo.CachedSpecimenSnapshot("s1"); ok {
		t.Fatal("stale specimen store must be rejected")
	}
}
