use anyhow::{Result, ensure};

use crate::calibration::{CalibrationProfile, CalibrationProvenance};
use crate::config::SimConfig;

pub(super) fn ensure_compatible_provenance(
    reference: &CalibrationProvenance,
    traffic: &CalibrationProvenance,
) -> Result<()> {
    ensure!(
        reference.hardware_profile == traffic.hardware_profile
            && reference.architecture == traffic.architecture
            && reference.cpu_cores_per_node == traffic.cpu_cores_per_node,
        "cpu reference ({} / {} / {} cores) and traffic profile ({} / {} / {} cores) come from \
         different machines; wall-minus-cpu subtraction across hardware is meaningless",
        reference.hardware_profile,
        reference.architecture,
        reference.cpu_cores_per_node,
        traffic.hardware_profile,
        traffic.architecture,
        traffic.cpu_cores_per_node,
    );
    ensure!(
        reference.deployment_revision == traffic.deployment_revision,
        "cpu reference revision {:?} and traffic profile revision {:?} differ; service-wall \
         subtraction across renderer revisions is meaningless",
        reference.deployment_revision,
        traffic.deployment_revision,
    );
    ensure!(
        reference.renderer_slots_per_node == traffic.renderer_slots_per_node
            && reference.execution_permits_per_node == traffic.execution_permits_per_node
            && reference.native_render_permits_per_node == traffic.native_render_permits_per_node,
        "cpu reference node shape ({} slots / {} execution / {} native) and traffic profile \
         node shape ({} slots / {} execution / {} native) differ; concurrent service walls \
         are not comparable",
        reference.renderer_slots_per_node,
        reference.execution_permits_per_node,
        reference.native_render_permits_per_node,
        traffic.renderer_slots_per_node,
        traffic.execution_permits_per_node,
        traffic.native_render_permits_per_node,
    );
    Ok(())
}
/// Apply the measured node shape before running with derived costs. Keeping
/// provenance only in the report would silently combine service times measured
/// on one machine/permit layout with the simulator's unrelated defaults.
pub fn apply_profile_provenance(
    profile: &CalibrationProfile,
    config: &mut SimConfig,
) -> Result<()> {
    let provenance = &profile.provenance;
    ensure!(
        provenance.cpu_cores_per_node > 0,
        "calibration profile has zero CPU cores per node"
    );
    ensure!(
        provenance.renderer_slots_per_node > 0,
        "calibration profile has zero renderer slots per node"
    );
    ensure!(
        provenance.execution_permits_per_node > 0
            && provenance.execution_permits_per_node <= provenance.renderer_slots_per_node,
        "calibration execution permits must be in 1..=renderer slots"
    );
    ensure!(
        provenance.native_render_permits_per_node > 0
            && provenance.native_render_permits_per_node <= provenance.renderer_slots_per_node,
        "calibration native-render permits must be in 1..=renderer slots"
    );
    config.cpu_cores_per_node = provenance.cpu_cores_per_node;
    config.cluster.renderer_slots_per_node = provenance.renderer_slots_per_node;
    config.cluster.render_permits_per_node = Some(provenance.execution_permits_per_node);
    config.cluster.native_render_permits_per_node = Some(provenance.native_render_permits_per_node);
    Ok(())
}
