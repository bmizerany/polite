// This is our main "CI tests" workflow. It runs everything that should run on
// both PRs and merged commits, and for the latter reports failures to slack.
name: "CI"

env: GO: "go1.22rc2"

on: {
	push: branches: [
		"main",
		"release-branch/*",
	]
	pull_request: null
	// all PRs on all branches
	merge_group: {
		branches: ["main"]
	}
}

concurrency: {
	// For PRs, later CI runs preempt previous ones. e.g. a force push on a PR
	// cancels running CI jobs and starts all new ones.
	//
	// For non-PR pushes, concurrency.group needs to be unique for every distinct
	// CI run we want to have happen. Use run_id, which in practice means all
	// non-PR CI runs will be allowed to run without preempting each other.
	group:                "${{ github.workflow }}-$${{ github.pull_request.number || github.run_id }}"
	"cancel-in-progress": true
}

jobs: {
	test: {
		env: GOEXPERIMENT: "rangefunc"
		"runs-on": "ubuntu-22.04"
		steps: [{
			name: "Install Go"
			if:   "${{ steps.install_go.outputs.cache-hit != 'true' }}"

			// Clear this so that the go install below doesn't
			// complain about unknown rangefunc.
			env: GOEXPERIMENT: ""
			run: """
				go env
				echo $PATH
				GOBIN=~/.local/bin go install golang.org/dl/$GO@latest
				$GO download
				"""
		}, {
			name: "Checkout"
			uses: "actions/checkout@v4"
		}, {
			name: "Restore Cache"
			uses: "actions/cache@v4"
			with: {
				// Note: unlike the other setups, this is only grabbing the mod download
				// cache, rather than the whole mod directory, as the download cache
				// contains zips that can be unpacked in parallel faster than they can be
				// fetched and extracted by tar
				path: """
					~/.cache/go-build
					~/go/pkg/mod/cache
					"""

				// The -2- here should be incremented when the scheme of data to be
				// cached changes (e.g. path above changes).
				key: "${{ github.job }}-${{ runner.os }}-go-2-${{ hashFiles('**/go.sum') }}-${{ github.run_id }}"
				"restore-keys": """
					${{ github.job }}-${{ runner.os }}-go-2-${{ hashFiles('**/go.sum') }}
					${{ github.job }}-${{ runner.os }}-go-2-
					"""
			}
		}, {
			uses: "ikalnytskyi/action-setup-postgres@v5"
			with: port: 5433
		}, {
			name: "go test"
			run:  "$GO test -v ./..."
		}, {
			name: "bench all"
			run:  "$GO test -bench=. -benchtime=1x -run=^$ $(for x in $(git grep -l \"^func Benchmark\" | xargs dirname | sort | uniq); do echo \"./$x\"; done)"
		}, {
			name: "check that no tracked files changed"
			run:  "git diff --no-ext-diff --name-only --exit-code || (echo \"Build/test modified the files above.\"; exit 1)"
		}, {
			name: "check that no new files were added"
			run: """
				# Note: The \"error: pathspec...\" you see below is normal!
				# In the success case in which there are no new untracked files,
				# git ls-files complains about the pathspec not matching anything.
				# That's OK. It's not worth the effort to suppress. Please ignore it.
				if git ls-files --others --exclude-standard --directory --no-empty-directory --error-unmatch -- ':/*'
				then
				  echo \"Build/test created untracked files in the repo (file names above).\"
				  exit 1
				fi

				"""
		}, {
			name: "check that 'go generate' is clean"
			run: """
				pkgs=$($GO list ./... | grep -Ev 'dnsfallback|k8s-operator')
				$GO generate $pkgs
				echo
				echo
				git diff --name-only --exit-code || (echo \"The files above need updating. Please run '$GO generate'.\"; exit 1)

				"""
		}, {
			name: "check that 'go mod tidy' is clean"
			run: """
				$GO mod tidy
				echo
				echo
				git diff --name-only --exit-code || (echo \"Please run 'go mod tidy'.\"; exit 1)

				"""
		}]
	}
}
