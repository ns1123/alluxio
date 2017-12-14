We vendor dependencies for generate-enterprise-tarballs using gb vendor

# Update a dependency
gb vendor update github.com/TachyonNexus/golluxio/annotation

# Add a dependency
gb vendor fetch -no-recurse github.com/TachyonNexus/golluxio/annotation

gb installs dependencies into vendor/src, but golang reads them from vendor. When you install
a new dependency, make sure there is a symlink in vendor/ that points to the dependency in
vendor/src.