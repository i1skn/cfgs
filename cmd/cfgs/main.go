package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
)

var scpLikeRemote = regexp.MustCompile(`^[^/\s]+@[^/\s:]+:.+`)

type app struct {
	in     *bufio.Reader
	out    io.Writer
	errOut io.Writer
}

type cfgsConfig struct {
	RepoPath    string   `json:"repo_path"`
	IgnoreGlobs []string `json:"ignore_globs,omitempty"`
}

type doctorReport struct {
	didNotTouch           []string
	replacedWithSymlink   []string
	unlinkedOrphanSymlink []string
	requireManualResolve  []string
}

type operationReport struct {
	changed   bool
	succeeded []string
	skipped   []string
	failed    []string
}

type globMatcher struct {
	pattern string
	regex   *regexp.Regexp
}

var defaultIgnoreGlobs = []string{
	"node_modules",
	"node_modules/**",
	"**/node_modules",
	"**/node_modules/**",
}

func main() {
	a := &app{
		in:     bufio.NewReader(os.Stdin),
		out:    os.Stdout,
		errOut: os.Stderr,
	}
	os.Exit(a.run(context.Background(), os.Args[1:]))
}

func (a *app) run(ctx context.Context, args []string) int {
	if len(args) == 0 {
		a.printUsage()
		return 1
	}

	if err := requireCommands("git", "fzf"); err != nil {
		fmt.Fprintf(a.errOut, "error: %v\n", err)
		return 1
	}

	// Always read cfgs config before dispatching any command.
	if _, _, err := loadCfgsConfig(); err != nil {
		fmt.Fprintf(a.errOut, "error: read cfgs config: %v\n", err)
		return 1
	}

	var err error
	switch args[0] {
	case "init":
		err = a.cmdInit(ctx)
	case "sync":
		err = a.cmdSync(ctx)
	case "add":
		err = a.cmdAdd(ctx)
	case "remove":
		err = a.cmdRemove(ctx)
	case "doctor":
		err = a.cmdDoctor(ctx)
	case "check":
		err = a.cmdCheck(ctx)
	case "unlink":
		err = a.cmdUnlink(ctx)
	case "help", "-h", "--help":
		a.printUsage()
		return 0
	default:
		a.printUsage()
		return 1
	}

	if err != nil {
		fmt.Fprintf(a.errOut, "error: %v\n", err)
		return 1
	}
	return 0
}

func (a *app) printUsage() {
	fmt.Fprintln(a.out, "Usage: cfgs <command>")
	fmt.Fprintln(a.out, "")
	fmt.Fprintln(a.out, "Commands:")
	fmt.Fprintln(a.out, "  init    Initialize cfgs repository and track selected files")
	fmt.Fprintln(a.out, "  sync    Pull latest from remote and run doctor")
	fmt.Fprintln(a.out, "  add     Add more config files from XDG_CONFIG_HOME")
	fmt.Fprintln(a.out, "  remove  Remove tracked files from repository and restore local copies")
	fmt.Fprintln(a.out, "  doctor  Reconcile symlinks between repo and XDG_CONFIG_HOME")
	fmt.Fprintln(a.out, "  check   Quick git clean check with optional commit/push")
	fmt.Fprintln(a.out, "  unlink  Replace tracked symlinks with local copies")
}

func (a *app) cmdInit(ctx context.Context) error {
	_ = ctx

	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("resolve home directory: %w", err)
	}
	defaultRepo := filepath.Join(home, ".cfgs")

	repoInput, err := a.promptLine("Repository path or remote URL", defaultRepo)
	if err != nil {
		return err
	}
	repoInput = expandPath(repoInput)

	var repoPath string
	if looksLikeRemote(repoInput) {
		dest, err := a.promptLine("Clone destination", defaultRepo)
		if err != nil {
			return err
		}
		dest = expandPath(dest)
		if err := ensureEmptyOrMissingDir(dest); err != nil {
			return err
		}
		if _, err := runCommand("", "git", "clone", repoInput, dest); err != nil {
			return err
		}
		repoPath = dest
	} else {
		repoPath = repoInput
	}

	repoPath, err = validateAndNormalizeRepo(repoPath)
	if err != nil {
		return err
	}
	cfg, ok, err := loadCfgsConfig()
	if err != nil {
		return err
	}
	ignoreGlobs := append([]string(nil), defaultIgnoreGlobs...)
	if ok && len(cfg.IgnoreGlobs) > 0 {
		ignoreGlobs = append([]string(nil), cfg.IgnoreGlobs...)
	}
	if err := saveCfgsConfig(cfgsConfig{
		RepoPath:    repoPath,
		IgnoreGlobs: ignoreGlobs,
	}); err != nil {
		return err
	}

	isEmpty, err := repoIsEmpty(repoPath)
	if err != nil {
		return err
	}
	if !isEmpty {
		fmt.Fprintln(a.out, "Repository is not empty; running doctor.")
		return a.cmdDoctorWithRepo(ctx, repoPath)
	}

	candidates, err := scanXDGRegularFiles()
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		fmt.Fprintln(a.out, "No files found in XDG_CONFIG_HOME.")
		return nil
	}

	selected, err := selectWithFzf(candidates, "init> ")
	if err != nil {
		return err
	}
	if len(selected) == 0 {
		fmt.Fprintln(a.out, "No files selected.")
		return nil
	}

	managed, err := loadManagedFiles(repoPath)
	if err != nil {
		return err
	}
	report, _ := trackSelections(repoPath, managed, selected)
	printOperationReport(a.out, "init", report)

	if report.changed {
		if err := a.commitAndAskPush(repoPath); err != nil {
			return err
		}
	}
	return nil
}

func (a *app) cmdSync(ctx context.Context) error {
	_ = ctx
	repoPath, err := a.resolveRepoPath()
	if err != nil {
		return err
	}
	beforeHead, beforeExists, err := gitHead(repoPath)
	if err != nil {
		return err
	}

	if _, err := runCommand(repoPath, "git", "pull", "--rebase", "--autostash"); err != nil {
		_, _ = runCommand(repoPath, "git", "rebase", "--abort")
		_, _ = runCommand(repoPath, "git", "merge", "--abort")
		return fmt.Errorf("sync failed; aborted any in-progress merge/rebase. Resolve manually with git pull + conflict resolution: %w", err)
	}
	afterHead, afterExists, err := gitHead(repoPath)
	if err != nil {
		return err
	}

	if err := a.showSyncDiff(repoPath, beforeHead, beforeExists, afterHead, afterExists); err != nil {
		return err
	}

	return a.cmdDoctorWithRepo(ctx, repoPath)
}

func (a *app) cmdAdd(ctx context.Context) error {
	_ = ctx
	repoPath, err := a.resolveRepoPath()
	if err != nil {
		return err
	}

	allXDGFiles, err := scanXDGRegularFiles()
	if err != nil {
		return err
	}
	managed, err := loadManagedFiles(repoPath)
	if err != nil {
		return err
	}
	managedSet := sliceToSet(managed)

	var candidates []string
	for _, rel := range allXDGFiles {
		if _, ok := managedSet[rel]; !ok {
			candidates = append(candidates, rel)
		}
	}
	sort.Strings(candidates)

	if len(candidates) == 0 {
		fmt.Fprintln(a.out, "No untracked files available to add.")
		return nil
	}

	selected, err := selectWithFzf(candidates, "add> ")
	if err != nil {
		return err
	}
	if len(selected) == 0 {
		fmt.Fprintln(a.out, "No files selected.")
		return nil
	}

	report, _ := trackSelections(repoPath, managed, selected)
	printOperationReport(a.out, "add", report)

	if report.changed {
		if err := a.commitAndAskPush(repoPath); err != nil {
			return err
		}
	}
	return nil
}

func (a *app) cmdRemove(ctx context.Context) error {
	_ = ctx
	repoPath, err := a.resolveRepoPath()
	if err != nil {
		return err
	}
	managed, err := loadManagedFiles(repoPath)
	if err != nil {
		return err
	}
	if len(managed) == 0 {
		fmt.Fprintln(a.out, "No tracked files to remove.")
		return nil
	}

	selected, err := selectWithFzf(managed, "remove> ")
	if err != nil {
		return err
	}
	if len(selected) == 0 {
		fmt.Fprintln(a.out, "No files selected.")
		return nil
	}

	xdg, err := xdgConfigHome()
	if err != nil {
		return err
	}

	report := operationReport{}

	for _, raw := range selected {
		rel, err := normalizeManagedPath(raw)
		if err != nil {
			report.failed = append(report.failed, fmt.Sprintf("%s: invalid path", raw))
			continue
		}

		repoFile := filepath.Join(repoPath, filepath.FromSlash(rel))
		liveFile := filepath.Join(xdg, filepath.FromSlash(rel))

		if _, err := os.Stat(repoFile); err != nil {
			report.failed = append(report.failed, fmt.Sprintf("%s: repo file missing", rel))
			continue
		}

		if err := ensureLiveCopyForRemove(repoFile, liveFile); err != nil {
			report.failed = append(report.failed, fmt.Sprintf("%s: %v", rel, err))
			continue
		}

		if err := os.Remove(repoFile); err != nil {
			report.failed = append(report.failed, fmt.Sprintf("%s: remove repo file: %v", rel, err))
			continue
		}
		removeEmptyDirsUpward(repoPath, filepath.Dir(repoFile))

		report.changed = true
		report.succeeded = append(report.succeeded, rel)
	}

	printOperationReport(a.out, "remove", report)

	if report.changed {
		if err := a.commitAndAskPush(repoPath); err != nil {
			return err
		}
	}
	return nil
}

func (a *app) cmdDoctor(ctx context.Context) error {
	_ = ctx
	repoPath, err := a.resolveRepoPath()
	if err != nil {
		return err
	}
	return a.cmdDoctorWithRepo(ctx, repoPath)
}

func (a *app) cmdDoctorWithRepo(ctx context.Context, repoPath string) error {
	_ = ctx

	managed, err := loadManagedFiles(repoPath)
	if err != nil {
		return err
	}
	if len(managed) == 0 {
		fmt.Fprintln(a.out, "No tracked files found.")
		return nil
	}

	xdg, err := xdgConfigHome()
	if err != nil {
		return err
	}

	report := doctorReport{}
	managedSet := sliceToSet(managed)

	for _, rel := range managed {
		repoFile := filepath.Join(repoPath, filepath.FromSlash(rel))
		liveFile := filepath.Join(xdg, filepath.FromSlash(rel))

		repoInfo, err := os.Stat(repoFile)
		if err != nil || !repoInfo.Mode().IsRegular() {
			report.requireManualResolve = append(report.requireManualResolve, rel)
			continue
		}

		liveInfo, err := os.Lstat(liveFile)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				report.requireManualResolve = append(report.requireManualResolve, rel)
				continue
			}
			if err := os.MkdirAll(filepath.Dir(liveFile), 0o755); err != nil {
				report.requireManualResolve = append(report.requireManualResolve, rel)
				continue
			}
			if err := os.Symlink(repoFile, liveFile); err != nil {
				report.requireManualResolve = append(report.requireManualResolve, rel)
				continue
			}
			report.replacedWithSymlink = append(report.replacedWithSymlink, rel)
			continue
		}

		if liveInfo.Mode()&os.ModeSymlink != 0 {
			ok, err := symlinkPointsTo(liveFile, repoFile)
			if err != nil {
				report.requireManualResolve = append(report.requireManualResolve, rel)
			} else if ok {
				report.didNotTouch = append(report.didNotTouch, rel)
			} else {
				report.requireManualResolve = append(report.requireManualResolve, rel)
			}
			continue
		}

		if !liveInfo.Mode().IsRegular() {
			report.requireManualResolve = append(report.requireManualResolve, rel)
			continue
		}

		same, err := filesEqual(repoFile, liveFile)
		if err != nil {
			report.requireManualResolve = append(report.requireManualResolve, rel)
			continue
		}
		if !same {
			report.requireManualResolve = append(report.requireManualResolve, rel)
			continue
		}

		if err := os.Remove(liveFile); err != nil {
			report.requireManualResolve = append(report.requireManualResolve, rel)
			continue
		}
		if err := os.Symlink(repoFile, liveFile); err != nil {
			report.requireManualResolve = append(report.requireManualResolve, rel)
			continue
		}
		report.replacedWithSymlink = append(report.replacedWithSymlink, rel)
	}

	ignoreMatchers, err := configuredIgnoreMatchers()
	if err != nil {
		return err
	}
	orphanReport, err := reconcileOrphanRepoSymlinks(repoPath, xdg, managedSet, ignoreMatchers)
	if err != nil {
		return err
	}
	report.unlinkedOrphanSymlink = append(report.unlinkedOrphanSymlink, orphanReport.unlinkedOrphanSymlink...)
	report.requireManualResolve = append(report.requireManualResolve, orphanReport.requireManualResolve...)

	printDoctorReport(a.out, report)

	if len(report.requireManualResolve) > 0 {
		return fmt.Errorf("manual reconcile required for %d file(s)", len(report.requireManualResolve))
	}
	return nil
}

func (a *app) cmdCheck(ctx context.Context) error {
	_ = ctx
	repoPath, err := a.resolveRepoPath()
	if err != nil {
		return err
	}

	dirty, err := gitIsDirty(repoPath)
	if err != nil {
		return err
	}
	if !dirty {
		fmt.Fprintln(a.out, "Git working tree is clean.")
		return nil
	}

	if err := a.showCheckDiff(repoPath); err != nil {
		return err
	}

	commitNow, err := a.promptYesNo("Uncommitted changes detected. Commit them now?", true)
	if err != nil {
		return err
	}
	if !commitNow {
		fmt.Fprintln(a.out, "Skipped commit.")
		return nil
	}

	if _, err := runCommand(repoPath, "git", "add", "-A"); err != nil {
		return err
	}
	if err := commitWithEditor(repoPath); err != nil {
		return err
	}

	pushNow, err := a.promptYesNo("Push commit now?", false)
	if err != nil {
		return err
	}
	if pushNow {
		if _, err := runCommand(repoPath, "git", "push"); err != nil {
			return err
		}
	}

	return a.cmdDoctorWithRepo(ctx, repoPath)
}

func (a *app) cmdUnlink(ctx context.Context) error {
	_ = ctx
	repoPath, err := a.resolveRepoPath()
	if err != nil {
		return err
	}
	managed, err := loadManagedFiles(repoPath)
	if err != nil {
		return err
	}
	if len(managed) == 0 {
		fmt.Fprintln(a.out, "No tracked files to unlink.")
		return nil
	}

	selected, err := selectWithFzf(managed, "unlink> ")
	if err != nil {
		return err
	}
	if len(selected) == 0 {
		fmt.Fprintln(a.out, "No files selected.")
		return nil
	}

	xdg, err := xdgConfigHome()
	if err != nil {
		return err
	}

	report := operationReport{}
	for _, raw := range selected {
		rel, err := normalizeManagedPath(raw)
		if err != nil {
			report.failed = append(report.failed, fmt.Sprintf("%s: invalid path", raw))
			continue
		}

		repoFile := filepath.Join(repoPath, filepath.FromSlash(rel))
		liveFile := filepath.Join(xdg, filepath.FromSlash(rel))
		liveInfo, err := os.Lstat(liveFile)
		if err != nil {
			report.skipped = append(report.skipped, fmt.Sprintf("%s: live file missing", rel))
			continue
		}
		if liveInfo.Mode()&os.ModeSymlink == 0 {
			report.skipped = append(report.skipped, fmt.Sprintf("%s: live file is not a symlink", rel))
			continue
		}

		ok, err := symlinkPointsTo(liveFile, repoFile)
		if err != nil || !ok {
			report.skipped = append(report.skipped, fmt.Sprintf("%s: symlink does not point to repo file", rel))
			continue
		}

		if err := os.Remove(liveFile); err != nil {
			report.failed = append(report.failed, fmt.Sprintf("%s: remove symlink: %v", rel, err))
			continue
		}
		if err := copyFile(repoFile, liveFile); err != nil {
			report.failed = append(report.failed, fmt.Sprintf("%s: copy file: %v", rel, err))
			continue
		}
		report.changed = true
		report.succeeded = append(report.succeeded, rel)
	}

	printOperationReport(a.out, "unlink", report)
	return nil
}

func (a *app) resolveRepoPath() (string, error) {
	if fromEnv := strings.TrimSpace(os.Getenv("CFGS_REPO")); fromEnv != "" {
		repoPath, err := validateAndNormalizeRepo(expandPath(fromEnv))
		if err != nil {
			return "", fmt.Errorf("CFGS_REPO: %w", err)
		}
		return repoPath, nil
	}

	if cfg, ok, err := loadCfgsConfig(); err != nil {
		return "", fmt.Errorf("read cfgs config: %w", err)
	} else if ok {
		repoPath, err := validateAndNormalizeRepo(cfg.RepoPath)
		if err != nil {
			return "", fmt.Errorf("cfgs config repo_path: %w", err)
		}
		return repoPath, nil
	}

	return "", fmt.Errorf("could not resolve repository (run `cfgs init`, set CFGS_REPO, or create $XDG_CONFIG_HOME/cfgs/config.json)")
}

func (a *app) commitAndAskPush(repoPath string) error {
	dirty, err := gitIsDirty(repoPath)
	if err != nil {
		return err
	}
	if !dirty {
		return nil
	}

	if _, err := runCommand(repoPath, "git", "add", "-A"); err != nil {
		return err
	}
	if err := commitWithEditor(repoPath); err != nil {
		return err
	}

	pushNow, err := a.promptYesNo("Push commit now?", false)
	if err != nil {
		return err
	}
	if pushNow {
		if _, err := runCommand(repoPath, "git", "push"); err != nil {
			return err
		}
	}
	return nil
}

func (a *app) showSyncDiff(repoPath string, beforeHead string, beforeExists bool, afterHead string, afterExists bool) error {
	switch {
	case beforeExists && afterExists && beforeHead == afterHead:
		fmt.Fprintln(a.out, "sync: already up to date.")
		return nil
	case beforeExists && afterExists:
		fmt.Fprintf(a.out, "sync: pulled updates (%s..%s)\n", shortHash(beforeHead), shortHash(afterHead))
		return runInteractiveCommand(repoPath, "git", "--no-pager", "diff", beforeHead+".."+afterHead)
	case !beforeExists && afterExists:
		fmt.Fprintf(a.out, "sync: repository now has commits; showing latest commit (%s)\n", shortHash(afterHead))
		return runInteractiveCommand(repoPath, "git", "--no-pager", "show", afterHead)
	default:
		fmt.Fprintln(a.out, "sync: no commits found.")
		return nil
	}
}

func (a *app) showCheckDiff(repoPath string) error {
	fmt.Fprintln(a.out, "check: git status --short")
	status, err := runCommand(repoPath, "git", "--no-pager", "status", "--short")
	if err != nil {
		return err
	}
	if strings.TrimSpace(status) == "" {
		fmt.Fprintln(a.out, "(no status lines)")
	} else {
		fmt.Fprintln(a.out, status)
	}

	hasHead, err := repoHasHead(repoPath)
	if err != nil {
		return err
	}

	if hasHead {
		fmt.Fprintln(a.out, "check: git diff HEAD")
		return runInteractiveCommand(repoPath, "git", "--no-pager", "diff", "HEAD")
	}

	fmt.Fprintln(a.out, "check: git diff")
	return runInteractiveCommand(repoPath, "git", "--no-pager", "diff")
}

func trackSelections(repoPath string, managed []string, selections []string) (operationReport, map[string]struct{}) {
	xdg, err := xdgConfigHome()
	if err != nil {
		return operationReport{
			failed: []string{fmt.Sprintf("resolve XDG_CONFIG_HOME: %v", err)},
		}, sliceToSet(managed)
	}

	managedSet := sliceToSet(managed)
	report := operationReport{}

	for _, raw := range selections {
		rel, err := normalizeManagedPath(raw)
		if err != nil {
			report.failed = append(report.failed, fmt.Sprintf("%s: invalid path", raw))
			continue
		}
		if _, exists := managedSet[rel]; exists {
			report.skipped = append(report.skipped, fmt.Sprintf("%s: already tracked", rel))
			continue
		}

		liveFile := filepath.Join(xdg, filepath.FromSlash(rel))
		repoFile := filepath.Join(repoPath, filepath.FromSlash(rel))

		liveInfo, err := os.Lstat(liveFile)
		if err != nil {
			report.failed = append(report.failed, fmt.Sprintf("%s: source file missing", rel))
			continue
		}
		if !liveInfo.Mode().IsRegular() {
			report.skipped = append(report.skipped, fmt.Sprintf("%s: source is not a regular file", rel))
			continue
		}

		if _, err := os.Stat(repoFile); err == nil {
			report.failed = append(report.failed, fmt.Sprintf("%s: repo file already exists", rel))
			continue
		} else if !errors.Is(err, fs.ErrNotExist) {
			report.failed = append(report.failed, fmt.Sprintf("%s: repo file check failed: %v", rel, err))
			continue
		}

		if err := os.MkdirAll(filepath.Dir(repoFile), 0o755); err != nil {
			report.failed = append(report.failed, fmt.Sprintf("%s: create repo dir: %v", rel, err))
			continue
		}

		if err := moveFile(liveFile, repoFile); err != nil {
			report.failed = append(report.failed, fmt.Sprintf("%s: move file: %v", rel, err))
			continue
		}

		if err := os.MkdirAll(filepath.Dir(liveFile), 0o755); err != nil {
			_ = moveFile(repoFile, liveFile)
			report.failed = append(report.failed, fmt.Sprintf("%s: restore after mkdir failure: %v", rel, err))
			continue
		}

		if err := os.Symlink(repoFile, liveFile); err != nil {
			_ = moveFile(repoFile, liveFile)
			report.failed = append(report.failed, fmt.Sprintf("%s: create symlink: %v", rel, err))
			continue
		}

		managedSet[rel] = struct{}{}
		report.changed = true
		report.succeeded = append(report.succeeded, rel)
	}

	return report, managedSet
}

func ensureLiveCopyForRemove(repoFile string, liveFile string) error {
	liveInfo, err := os.Lstat(liveFile)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("inspect live file: %w", err)
		}
		if err := os.MkdirAll(filepath.Dir(liveFile), 0o755); err != nil {
			return fmt.Errorf("create live dir: %w", err)
		}
		if err := copyFile(repoFile, liveFile); err != nil {
			return fmt.Errorf("copy repo file to live location: %w", err)
		}
		return nil
	}

	if liveInfo.Mode()&os.ModeSymlink != 0 {
		ok, err := symlinkPointsTo(liveFile, repoFile)
		if err != nil {
			return fmt.Errorf("inspect symlink: %w", err)
		}
		if !ok {
			return fmt.Errorf("live symlink points elsewhere")
		}
		if err := os.Remove(liveFile); err != nil {
			return fmt.Errorf("remove live symlink: %w", err)
		}
		if err := copyFile(repoFile, liveFile); err != nil {
			return fmt.Errorf("copy repo file to live location: %w", err)
		}
		return nil
	}

	if !liveInfo.Mode().IsRegular() {
		return fmt.Errorf("live path is not a regular file")
	}
	return nil
}

func printOperationReport(w io.Writer, action string, report operationReport) {
	fmt.Fprintf(w, "%s summary:\n", action)

	fmt.Fprintln(w, "  succeeded:")
	if len(report.succeeded) == 0 {
		fmt.Fprintln(w, "    (none)")
	} else {
		for _, item := range report.succeeded {
			fmt.Fprintf(w, "    - %s\n", item)
		}
	}

	fmt.Fprintln(w, "  skipped:")
	if len(report.skipped) == 0 {
		fmt.Fprintln(w, "    (none)")
	} else {
		for _, item := range report.skipped {
			fmt.Fprintf(w, "    - %s\n", item)
		}
	}

	fmt.Fprintln(w, "  failed:")
	if len(report.failed) == 0 {
		fmt.Fprintln(w, "    (none)")
	} else {
		for _, item := range report.failed {
			fmt.Fprintf(w, "    - %s\n", item)
		}
	}
}

func printDoctorReport(w io.Writer, report doctorReport) {
	fmt.Fprintln(w, "did not touch:")
	if len(report.didNotTouch) == 0 {
		fmt.Fprintln(w, "  (none)")
	} else {
		for _, item := range report.didNotTouch {
			fmt.Fprintf(w, "  - %s\n", item)
		}
	}

	fmt.Fprintln(w, "replaced with symlink:")
	if len(report.replacedWithSymlink) == 0 {
		fmt.Fprintln(w, "  (none)")
	} else {
		for _, item := range report.replacedWithSymlink {
			fmt.Fprintf(w, "  - %s\n", item)
		}
	}

	fmt.Fprintln(w, "unlinked orphan symlink:")
	if len(report.unlinkedOrphanSymlink) == 0 {
		fmt.Fprintln(w, "  (none)")
	} else {
		for _, item := range report.unlinkedOrphanSymlink {
			fmt.Fprintf(w, "  - %s\n", item)
		}
	}

	fmt.Fprintln(w, "require manual reconcile:")
	if len(report.requireManualResolve) == 0 {
		fmt.Fprintln(w, "  (none)")
	} else {
		for _, item := range report.requireManualResolve {
			fmt.Fprintf(w, "  - %s\n", item)
		}
	}
}

func reconcileOrphanRepoSymlinks(repoPath string, xdg string, managed map[string]struct{}, ignoreMatchers []globMatcher) (doctorReport, error) {
	report := doctorReport{}
	repoPath = filepath.Clean(repoPath)

	err := filepath.WalkDir(xdg, func(fullPath string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}

		rel, err := filepath.Rel(xdg, fullPath)
		if err != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)

		if d.IsDir() {
			if shouldIgnorePath(rel, true, ignoreMatchers) {
				return filepath.SkipDir
			}
			return nil
		}
		if shouldIgnorePath(rel, false, ignoreMatchers) {
			return nil
		}
		if d.Type()&os.ModeSymlink == 0 {
			return nil
		}

		rel, err = normalizeManagedPath(rel)
		if err != nil {
			return nil
		}
		if _, ok := managed[rel]; ok {
			return nil
		}

		target, inRepo, err := symlinkRepoTarget(fullPath, repoPath)
		if err != nil || !inRepo {
			return nil
		}

		targetInfo, err := os.Stat(target)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				// Repo file is gone; remove dangling link as unlink behavior.
				if err := os.Remove(fullPath); err != nil {
					report.requireManualResolve = append(report.requireManualResolve, rel)
					return nil
				}
				report.unlinkedOrphanSymlink = append(report.unlinkedOrphanSymlink, rel+" (removed dangling symlink)")
				return nil
			}
			report.requireManualResolve = append(report.requireManualResolve, rel)
			return nil
		}
		if !targetInfo.Mode().IsRegular() {
			report.requireManualResolve = append(report.requireManualResolve, rel)
			return nil
		}

		if err := os.Remove(fullPath); err != nil {
			report.requireManualResolve = append(report.requireManualResolve, rel)
			return nil
		}
		if err := copyFile(target, fullPath); err != nil {
			report.requireManualResolve = append(report.requireManualResolve, rel)
			return nil
		}
		report.unlinkedOrphanSymlink = append(report.unlinkedOrphanSymlink, rel)
		return nil
	})
	if err != nil {
		return doctorReport{}, err
	}

	sort.Strings(report.unlinkedOrphanSymlink)
	sort.Strings(report.requireManualResolve)
	return report, nil
}

func symlinkRepoTarget(linkPath string, repoPath string) (string, bool, error) {
	rawTarget, err := os.Readlink(linkPath)
	if err != nil {
		return "", false, err
	}

	targetPath := rawTarget
	if !filepath.IsAbs(targetPath) {
		targetPath = filepath.Join(filepath.Dir(linkPath), targetPath)
	}
	targetPath = filepath.Clean(targetPath)

	withinRepo, err := pathWithin(repoPath, targetPath)
	if err != nil || !withinRepo {
		return "", false, err
	}
	return targetPath, true, nil
}

func pathWithin(base string, candidate string) (bool, error) {
	rel, err := filepath.Rel(filepath.Clean(base), filepath.Clean(candidate))
	if err != nil {
		return false, err
	}
	if rel == "." {
		return true, nil
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return false, nil
	}
	return true, nil
}

func requireCommands(commands ...string) error {
	var missing []string
	for _, command := range commands {
		if _, err := exec.LookPath(command); err != nil {
			missing = append(missing, command)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required commands: %s", strings.Join(missing, ", "))
	}
	return nil
}

func runCommand(dir string, name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s %s failed: %w\n%s", name, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return strings.TrimSpace(string(output)), nil
}

func runInteractiveCommand(dir string, name string, args ...string) error {
	cmd := exec.Command(name, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %s failed: %w", name, strings.Join(args, " "), err)
	}
	return nil
}

func commitWithEditor(repoPath string) error {
	fmt.Println("Opening editor for commit message...")
	return runInteractiveCommand(repoPath, "git", "commit")
}

func gitRepoRoot(path string) (string, error) {
	return runCommand(path, "git", "rev-parse", "--show-toplevel")
}

func gitHead(repoPath string) (string, bool, error) {
	hasHead, err := repoHasHead(repoPath)
	if err != nil {
		return "", false, err
	}
	if !hasHead {
		return "", false, nil
	}
	head, err := runCommand(repoPath, "git", "rev-parse", "HEAD")
	if err != nil {
		return "", false, err
	}
	return strings.TrimSpace(head), true, nil
}

func shortHash(commit string) string {
	if len(commit) <= 12 {
		return commit
	}
	return commit[:12]
}

func validateAndNormalizeRepo(repoPath string) (string, error) {
	repoPath = expandPath(repoPath)
	info, err := os.Stat(repoPath)
	if err != nil {
		return "", fmt.Errorf("repository path not found: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("repository path is not a directory")
	}

	root, err := gitRepoRoot(repoPath)
	if err != nil {
		return "", fmt.Errorf("path is not a git repository: %w", err)
	}
	if err := requireRepoRemote(root); err != nil {
		return "", err
	}
	return root, nil
}

func requireRepoRemote(repoPath string) error {
	remotes, err := runCommand(repoPath, "git", "remote")
	if err != nil {
		return err
	}
	if strings.TrimSpace(remotes) == "" {
		return fmt.Errorf("repository has no remote configured")
	}
	return nil
}

func repoIsEmpty(repoPath string) (bool, error) {
	hasHead, err := repoHasHead(repoPath)
	if err != nil {
		return false, err
	}
	if !hasHead {
		return true, nil
	}

	tracked, err := gitTrackedFiles(repoPath)
	if err != nil {
		return false, err
	}
	for _, rel := range tracked {
		if !isMetadataPath(rel) {
			return false, nil
		}
	}
	return true, nil
}

func repoHasHead(repoPath string) (bool, error) {
	cmd := exec.Command("git", "rev-parse", "--verify", "HEAD")
	cmd.Dir = repoPath
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func gitTrackedFiles(repoPath string) ([]string, error) {
	out, err := runCommand(repoPath, "git", "ls-files")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(out) == "" {
		return nil, nil
	}
	lines := strings.Split(out, "\n")
	var files []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		rel, err := normalizeManagedPath(line)
		if err != nil {
			continue
		}
		files = append(files, rel)
	}
	sort.Strings(files)
	return unique(files), nil
}

func gitIsDirty(repoPath string) (bool, error) {
	out, err := runCommand(repoPath, "git", "status", "--porcelain")
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(out) != "", nil
}

func selectWithFzf(items []string, prompt string) ([]string, error) {
	if len(items) == 0 {
		return nil, nil
	}

	xdg, err := xdgConfigHome()
	if err != nil {
		return nil, err
	}

	var input bytes.Buffer
	for _, item := range items {
		input.WriteString(item)
		input.WriteByte('\n')
	}

	preview := `p="$XDG_CONFIG_HOME"/{}; if [ -f "$p" ]; then (bat --style=plain --color=always --line-range=:200 "$p" 2>/dev/null || sed -n "1,200p" "$p"); else echo "No preview: $p"; fi`
	cmd := exec.Command(
		"fzf",
		"--multi",
		"--prompt", prompt,
		"--preview", preview,
		"--preview-window", "right,60%,border-left,wrap",
	)
	cmd.Stdin = &input
	cmd.Env = append(os.Environ(), "XDG_CONFIG_HOME="+xdg)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			code := exitErr.ExitCode()
			if code == 1 || code == 130 {
				return nil, nil
			}
		}
		errText := strings.TrimSpace(stderr.String())
		if errText == "" {
			errText = err.Error()
		}
		return nil, fmt.Errorf("fzf failed: %s", errText)
	}

	out := strings.TrimSpace(stdout.String())
	if out == "" {
		return nil, nil
	}

	lines := strings.Split(out, "\n")
	var selected []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		selected = append(selected, line)
	}
	sort.Strings(selected)
	return unique(selected), nil
}

func scanXDGRegularFiles() ([]string, error) {
	xdg, err := xdgConfigHome()
	if err != nil {
		return nil, err
	}
	ignoreMatchers, err := configuredIgnoreMatchers()
	if err != nil {
		return nil, err
	}

	var files []string
	err = filepath.WalkDir(xdg, func(fullPath string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}

		rel, err := filepath.Rel(xdg, fullPath)
		if err != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)

		if d.IsDir() {
			if shouldIgnorePath(rel, true, ignoreMatchers) {
				return filepath.SkipDir
			}
			return nil
		}
		if shouldIgnorePath(rel, false, ignoreMatchers) {
			return nil
		}

		mode := d.Type()
		if !mode.IsRegular() {
			info, err := d.Info()
			if err != nil || !info.Mode().IsRegular() {
				return nil
			}
		}

		normalized, err := normalizeManagedPath(rel)
		if err != nil {
			return nil
		}
		files = append(files, normalized)
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Strings(files)
	return unique(files), nil
}

func xdgConfigHome() (string, error) {
	if configured := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME")); configured != "" {
		return configured, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return filepath.Join(home, ".config"), nil
}

func looksLikeRemote(input string) bool {
	switch {
	case strings.HasPrefix(input, "http://"):
		return true
	case strings.HasPrefix(input, "https://"):
		return true
	case strings.HasPrefix(input, "ssh://"):
		return true
	case strings.HasPrefix(input, "git@"):
		return true
	case scpLikeRemote.MatchString(input):
		return true
	default:
		return false
	}
}

func expandPath(input string) string {
	input = strings.TrimSpace(input)
	if input == "~" {
		if home, err := os.UserHomeDir(); err == nil {
			return home
		}
	}
	if strings.HasPrefix(input, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, input[2:])
		}
	}
	return input
}

func ensureEmptyOrMissingDir(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("destination exists and is not a directory: %s", path)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return fmt.Errorf("destination exists and is not empty: %s", path)
	}
	return nil
}

func cfgsConfigPath() (string, error) {
	xdg, err := xdgConfigHome()
	if err != nil {
		return "", err
	}
	return filepath.Join(xdg, "cfgs", "config.json"), nil
}

func loadCfgsConfig() (cfgsConfig, bool, error) {
	configPath, err := cfgsConfigPath()
	if err != nil {
		return cfgsConfig{}, false, err
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return cfgsConfig{}, false, nil
		}
		return cfgsConfig{}, false, err
	}

	var cfg cfgsConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfgsConfig{}, false, err
	}
	cfg.RepoPath = strings.TrimSpace(cfg.RepoPath)
	cfg.IgnoreGlobs = sanitizeIgnoreGlobs(cfg.IgnoreGlobs)
	if cfg.RepoPath == "" {
		return cfgsConfig{}, false, nil
	}
	return cfg, true, nil
}

func saveCfgsConfig(cfg cfgsConfig) error {
	configPath, err := cfgsConfigPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		return err
	}
	cfg.RepoPath = strings.TrimSpace(cfg.RepoPath)
	cfg.IgnoreGlobs = sanitizeIgnoreGlobs(cfg.IgnoreGlobs)
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	return os.WriteFile(configPath, append(data, '\n'), 0o644)
}

func configuredIgnoreMatchers() ([]globMatcher, error) {
	cfg, ok, err := loadCfgsConfig()
	if err != nil {
		return nil, err
	}
	patterns := defaultIgnoreGlobs
	if ok && len(cfg.IgnoreGlobs) > 0 {
		patterns = cfg.IgnoreGlobs
	}
	return compileGlobMatchers(patterns)
}

func sanitizeIgnoreGlobs(patterns []string) []string {
	var out []string
	for _, pattern := range patterns {
		p := strings.TrimSpace(pattern)
		if p == "" {
			continue
		}
		p = strings.ReplaceAll(p, "\\", "/")
		p = strings.TrimPrefix(p, "./")
		out = append(out, p)
	}
	sort.Strings(out)
	return unique(out)
}

func compileGlobMatchers(patterns []string) ([]globMatcher, error) {
	patterns = sanitizeIgnoreGlobs(patterns)
	matchers := make([]globMatcher, 0, len(patterns))
	for _, pattern := range patterns {
		src, err := globToRegex(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid ignore glob %q: %w", pattern, err)
		}
		re, err := regexp.Compile(src)
		if err != nil {
			return nil, fmt.Errorf("invalid ignore glob %q: %w", pattern, err)
		}
		matchers = append(matchers, globMatcher{
			pattern: pattern,
			regex:   re,
		})
	}
	return matchers, nil
}

func globToRegex(pattern string) (string, error) {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return "", fmt.Errorf("empty pattern")
	}

	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]
		switch ch {
		case '*':
			if i+1 < len(pattern) && pattern[i+1] == '*' {
				b.WriteString(".*")
				i++
			} else {
				b.WriteString("[^/]*")
			}
		case '?':
			b.WriteString("[^/]")
		default:
			if strings.ContainsRune(`.+()|[]{}^$\\`, rune(ch)) {
				b.WriteByte('\\')
			}
			b.WriteByte(ch)
		}
	}
	b.WriteString("$")
	return b.String(), nil
}

func shouldIgnorePath(rel string, isDir bool, matchers []globMatcher) bool {
	rel = strings.TrimSpace(filepath.ToSlash(rel))
	if rel == "" || rel == "." {
		return false
	}
	for _, matcher := range matchers {
		if matcher.regex.MatchString(rel) {
			return true
		}
		if isDir && matcher.regex.MatchString(rel+"/") {
			return true
		}
	}
	return false
}

func loadManagedFiles(repoPath string) ([]string, error) {
	tracked, err := gitTrackedFiles(repoPath)
	if err != nil {
		return nil, err
	}
	var managed []string
	for _, rel := range tracked {
		if isMetadataPath(rel) {
			continue
		}
		managed = append(managed, rel)
	}
	sort.Strings(managed)
	return unique(managed), nil
}

func normalizeManagedPath(rel string) (string, error) {
	rel = filepath.ToSlash(strings.TrimSpace(rel))
	rel = path.Clean(rel)
	if rel == "." || rel == "" {
		return "", fmt.Errorf("invalid path")
	}
	if strings.HasPrefix(rel, "/") {
		return "", fmt.Errorf("absolute paths are not allowed")
	}
	if strings.HasPrefix(rel, "../") || strings.Contains(rel, "/../") {
		return "", fmt.Errorf("path traversal is not allowed")
	}
	if isMetadataPath(rel) {
		return "", fmt.Errorf("path is reserved")
	}
	return rel, nil
}

func isMetadataPath(rel string) bool {
	return rel == ".git" ||
		strings.HasPrefix(rel, ".git/")
}

func sliceToSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, v := range values {
		set[v] = struct{}{}
	}
	return set
}

func unique(values []string) []string {
	if len(values) == 0 {
		return values
	}
	sort.Strings(values)
	out := values[:1]
	for i := 1; i < len(values); i++ {
		if values[i] != values[i-1] {
			out = append(out, values[i])
		}
	}
	return out
}

func moveFile(src string, dst string) error {
	if err := os.Rename(src, dst); err == nil {
		return nil
	} else if !errors.Is(err, syscall.EXDEV) {
		return err
	}

	if err := copyFile(src, dst); err != nil {
		return err
	}
	return os.Remove(src)
}

func copyFile(src string, dst string) error {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !srcInfo.Mode().IsRegular() {
		return fmt.Errorf("source is not a regular file")
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}

	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	target, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, srcInfo.Mode().Perm())
	if err != nil {
		return err
	}
	defer target.Close()

	if _, err := io.Copy(target, source); err != nil {
		return err
	}
	return nil
}

func filesEqual(left string, right string) (bool, error) {
	leftData, err := os.ReadFile(left)
	if err != nil {
		return false, err
	}
	rightData, err := os.ReadFile(right)
	if err != nil {
		return false, err
	}
	return bytes.Equal(leftData, rightData), nil
}

func symlinkPointsTo(linkPath string, targetPath string) (bool, error) {
	resolved, err := filepath.EvalSymlinks(linkPath)
	if err != nil {
		return false, err
	}
	resolvedAbs, err := filepath.Abs(resolved)
	if err != nil {
		return false, err
	}
	targetAbs, err := filepath.Abs(targetPath)
	if err != nil {
		return false, err
	}
	return filepath.Clean(resolvedAbs) == filepath.Clean(targetAbs), nil
}

func removeEmptyDirsUpward(root string, dir string) {
	root = filepath.Clean(root)
	dir = filepath.Clean(dir)
	for {
		if dir == root || dir == "." || dir == "/" {
			return
		}
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) > 0 {
			return
		}
		if err := os.Remove(dir); err != nil {
			return
		}
		dir = filepath.Dir(dir)
	}
}

func (a *app) promptLine(label string, defaultValue string) (string, error) {
	if defaultValue != "" {
		fmt.Fprintf(a.out, "%s [%s]: ", label, defaultValue)
	} else {
		fmt.Fprintf(a.out, "%s: ", label)
	}

	text, err := a.in.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return defaultValue, nil
	}
	return text, nil
}

func (a *app) promptYesNo(question string, defaultYes bool) (bool, error) {
	var suffix string
	if defaultYes {
		suffix = "[Y/n]"
	} else {
		suffix = "[y/N]"
	}

	for {
		fmt.Fprintf(a.out, "%s %s: ", question, suffix)
		text, err := a.in.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return false, err
		}
		text = strings.TrimSpace(strings.ToLower(text))
		if text == "" {
			return defaultYes, nil
		}
		switch text {
		case "y", "yes":
			return true, nil
		case "n", "no":
			return false, nil
		default:
			fmt.Fprintln(a.out, "Please answer y or n.")
		}
	}
}
