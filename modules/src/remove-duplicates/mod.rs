pub struct HubModuleEntry {
    //
}

pub trait HubPipelineModule {
    pub fn filter(entry: &HubModuleEntry) -> Option<bool>;
}
