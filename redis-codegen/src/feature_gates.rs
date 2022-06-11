use crate::commands::CommandGroup;

static FEATURE_BY_GROUPS: &[(CommandGroup, &str)] = &[
    (CommandGroup::Stream, "streams"),
    (CommandGroup::Geo, "geospatial"),
];
static FEATURE_BY_NAME: &[(&str, &str)] = &[("ACL", "acl")];

pub(crate) trait FeatureGate {
    fn to_feature(&self) -> Option<&str>;
}

impl FeatureGate for CommandGroup {
    fn to_feature(&self) -> Option<&str> {
        FEATURE_BY_GROUPS
            .iter()
            .find(|&(group, _)| group == self)
            .map(|&(_, feature)| feature)
    }
}

impl FeatureGate for &str {
    fn to_feature(&self) -> Option<&str> {
        FEATURE_BY_NAME
            .iter()
            .find(|&(name, _)| self.starts_with(*name))
            .map(|&(_, feature)| feature)
    }
}
