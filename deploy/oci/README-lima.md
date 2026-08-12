# Optional: local OL9 aarch64 cloud-init smoke (Lima)
#
# Proves Docker bootstrap without OCI compute spend. Not a substitute for OCI
# networking (IGW) checks — only validates the same #cloud-config install path.
#
# Prerequisites: Lima (https://lima-vm.io/), Apple Silicon recommended (Ampere arch).
#
# 1) Download an Oracle Linux 9 aarch64 cloud image from Oracle's cloud image page.
# 2) Create a Lima YAML that sets user-data to this repo's cloud-init file, e.g.:
#
#    images:
#      - location: "/path/to/OL9-cloud.qcow2"
#        arch: "aarch64"
#    cloud-init:
#      userData: |
#        <paste contents of deploy/oci/cloud-init.yaml>
#
#    Or mount the repo and point cloud-init at the file per Lima docs for your version.
#
# 3) limactl start ol9-ct-firehose
# 4) limactl shell ol9-ct-firehose
# 5) Pass criteria (same as docs/DEPLOY.md):
#      test -f /var/lib/ct-firehose-filter/.bootstrap-complete
#      docker version && docker compose version
#      cat /etc/ct-firehose-filter/README
#      cloud-init status --wait   # should be "done" without package fatals
#
# Multipass (Ubuntu) is NOT valid for this bootstrap (dnf / Docker CE RHEL repo).
