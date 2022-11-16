const STREAM_SUBJECT_COMMMON_PREFIX: &str = "UPDATES.STORAGE";
const STREAM_SUBJECT_OBJECT_NAME: &str = "OBJECT";
const STREAM_SUBJECT_OBJECT_GROUP_NAME: &str = "OBJECTGROUP";

// Utility functions for Nats.io
pub struct NatsIOUtils {}

impl NatsIOUtils {
    // Creates the base subject for Nats.io subject strings that can be used to publish and query events
    // Each subject starts with a common prefix
    // Then the resource ids are added in the following order: PROJECT, COLLECTION, SHAREDOBJECT|SHAREDOBJECTGROUP, OBJECT|OBJECTGROUP
    // All ids are splited by the following symbol: ._.
    // This is required for proper queries
    // Between a collection and an SHAREDOBJECT|SHAREDOBJECTGROUP id there is an additional denominator that indicates if
    // the following id is and SHAREDOBJECT or SHAREDOBJECTGROUP id. The denominator is simply the resource name splitted by ._.
    fn base_subject(ids: Vec<String>, is_object_group: bool) -> String {
        let mut stage = 0;
        let mut base_subject = format!("{}", STREAM_SUBJECT_COMMMON_PREFIX);
        for id in ids {
            if stage == 2 {
                if is_object_group {
                    base_subject =
                        format!("{}._.{}", base_subject, STREAM_SUBJECT_OBJECT_GROUP_NAME);
                } else {
                    base_subject = format!("{}._.{}", base_subject, STREAM_SUBJECT_OBJECT_NAME);
                }
            }
            base_subject = format!("{}._.{}", base_subject, id);
            stage += 1;
        }
        return base_subject;
    }

    // Turns a subject into a query, depending on whether subresources are included or not
    fn query(base_subject: String, include_subresources: bool) -> String {
        let query = match include_subresources {
            true => format!("{}.>", base_subject),
            false => format!("{}._", base_subject),
        };

        return query;
    }

    pub fn project_subject(project_id: String) -> String {
        let subject = format!("{}._", NatsIOUtils::base_subject(vec![project_id], false));
        return subject;
    }

    pub fn project_query(project_id: String, include_subresources: bool) -> String {
        let base_subject = NatsIOUtils::base_subject(vec![project_id], false);
        let query = NatsIOUtils::query(base_subject, include_subresources);

        return query;
    }

    pub fn collection_subject(project_id: String, collection_id: String) -> String {
        let subject = format!(
            "{}._",
            NatsIOUtils::base_subject(vec![project_id, collection_id], false)
        );
        return subject;
    }

    pub fn collection_query(
        project_id: String,
        collection_id: String,
        include_subresources: bool,
    ) -> String {
        let base_subject = NatsIOUtils::base_subject(vec![project_id, collection_id], false);
        let query = NatsIOUtils::query(base_subject, include_subresources);

        return query;
    }

    pub fn object_subject(
        project_id: String,
        collection_id: String,
        shared_object_id: String,
        object_id: String,
    ) -> String {
        let subject = format!(
            "{}._",
            NatsIOUtils::base_subject(
                vec![project_id, collection_id, shared_object_id, object_id],
                false
            )
        );
        return subject;
    }

    pub fn object_query(
        project_id: String,
        collection_id: String,
        shared_object_id: String,
        object_id: String,
        include_subresources: bool,
    ) -> String {
        let base_subject = NatsIOUtils::base_subject(
            vec![project_id, collection_id, shared_object_id, object_id],
            false,
        );
        let query = NatsIOUtils::query(base_subject, include_subresources);

        return query;
    }

    pub fn object_group_subject(
        project_id: String,
        collection_id: String,
        shared_object_group_id: String,
        object_group_id: String,
    ) -> String {
        let subject = format!(
            "{}._",
            NatsIOUtils::base_subject(
                vec![
                    project_id,
                    collection_id,
                    shared_object_group_id,
                    object_group_id
                ],
                true
            )
        );
        return subject;
    }

    pub fn object_group_query(
        project_id: String,
        collection_id: String,
        shared_object_group_id: String,
        object_group_id: String,
        include_subresources: bool,
    ) -> String {
        let base_subject = NatsIOUtils::base_subject(
            vec![
                project_id,
                collection_id,
                shared_object_group_id,
                object_group_id,
            ],
            true,
        );
        let query = NatsIOUtils::query(base_subject, include_subresources);

        return query;
    }
}

#[cfg(test)]
mod tests {
    use crate::utils;

    #[test]
    fn test_base_subject() {
        let project_base_subject =
            utils::utils::NatsIOUtils::base_subject(vec!["project_id".to_string()], false);
        let collection_base_subject = utils::utils::NatsIOUtils::base_subject(
            vec!["project_id".to_string(), "collection_id".to_string()],
            false,
        );
        let object_base_subject = utils::utils::NatsIOUtils::base_subject(
            vec![
                "project_id".to_string(),
                "collection_id".to_string(),
                "shared_object_id".to_string(),
                "object_id".to_string(),
            ],
            false,
        );
        let object_group_base_subject = utils::utils::NatsIOUtils::base_subject(
            vec![
                "project_id".to_string(),
                "collection_id".to_string(),
                "shared_object_group_id".to_string(),
                "object_group_id".to_string(),
            ],
            true,
        );

        assert_eq!(project_base_subject, "UPDATES.STORAGE._.project_id");
        assert_eq!(
            collection_base_subject,
            "UPDATES.STORAGE._.project_id._.collection_id"
        );
        assert_eq!(
            object_base_subject,
            "UPDATES.STORAGE._.project_id._.collection_id._.OBJECT._.shared_object_id._.object_id"
        );
        assert_eq!(
            object_group_base_subject,
            "UPDATES.STORAGE._.project_id._.collection_id._.OBJECTGROUP._.shared_object_group_id._.object_group_id"
        );
    }

    #[test]
    fn test_query_strings() {
        let project_query =
            utils::utils::NatsIOUtils::project_query("project_id".to_string(), false);
        let project_query_sub =
            utils::utils::NatsIOUtils::project_query("project_id".to_string(), true);
        let collection_query = utils::utils::NatsIOUtils::collection_query(
            "project_id".to_string(),
            "collection_id".to_string(),
            false,
        );
        let collection_query_sub = utils::utils::NatsIOUtils::collection_query(
            "project_id".to_string(),
            "collection_id".to_string(),
            true,
        );
        let object_query = utils::utils::NatsIOUtils::object_query(
            "project_id".to_string(),
            "collection_id".to_string(),
            "shared_object_id".to_string(),
            "object_id".to_string(),
            false,
        );
        let object_query_sub = utils::utils::NatsIOUtils::object_query(
            "project_id".to_string(),
            "collection_id".to_string(),
            "shared_object_id".to_string(),
            "object_id".to_string(),
            true,
        );
        let object_group_query = utils::utils::NatsIOUtils::object_group_query(
            "project_id".to_string(),
            "collection_id".to_string(),
            "shared_object_group_id".to_string(),
            "object_group_id".to_string(),
            false,
        );
        let object_group_query_sub = utils::utils::NatsIOUtils::object_group_query(
            "project_id".to_string(),
            "collection_id".to_string(),
            "shared_object_group_id".to_string(),
            "object_group_id".to_string(),
            true,
        );

        assert_eq!(project_query, "UPDATES.STORAGE._.project_id._");
        assert_eq!(project_query_sub, "UPDATES.STORAGE._.project_id.>");
        assert_eq!(
            collection_query,
            "UPDATES.STORAGE._.project_id._.collection_id._"
        );
        assert_eq!(
            collection_query_sub,
            "UPDATES.STORAGE._.project_id._.collection_id.>"
        );
        assert_eq!(
            object_query,
            "UPDATES.STORAGE._.project_id._.collection_id._.OBJECT._.shared_object_id._.object_id._"
        );
        assert_eq!(
            object_query_sub,
            "UPDATES.STORAGE._.project_id._.collection_id._.OBJECT._.shared_object_id._.object_id.>"
        );
        assert_eq!(
            object_group_query,
            "UPDATES.STORAGE._.project_id._.collection_id._.OBJECTGROUP._.shared_object_group_id._.object_group_id._"
        );
        assert_eq!(object_group_query_sub, "UPDATES.STORAGE._.project_id._.collection_id._.OBJECTGROUP._.shared_object_group_id._.object_group_id.>")
    }

    #[test]
    fn test_subject_strings() {
        let project_subject = utils::utils::NatsIOUtils::project_subject("project_id".to_string());
        let collection_subject = utils::utils::NatsIOUtils::collection_subject(
            "project_id".to_string(),
            "collection_id".to_string(),
        );
        let object_subject = utils::utils::NatsIOUtils::object_subject(
            "project_id".to_string(),
            "collection_id".to_string(),
            "shared_object_id".to_string(),
            "object_id".to_string(),
        );
        let object_group_subject = utils::utils::NatsIOUtils::object_group_subject(
            "project_id".to_string(),
            "collection_id".to_string(),
            "shared_object_group_id".to_string(),
            "object_group_id".to_string(),
        );

        assert_eq!(project_subject, "UPDATES.STORAGE._.project_id._");
        assert_eq!(
            collection_subject,
            "UPDATES.STORAGE._.project_id._.collection_id._"
        );
        assert_eq!(
            object_subject,
            "UPDATES.STORAGE._.project_id._.collection_id._.OBJECT._.shared_object_id._.object_id._"
        );
        assert_eq!(
            object_group_subject,
            "UPDATES.STORAGE._.project_id._.collection_id._.OBJECTGROUP._.shared_object_group_id._.object_group_id._"
        );
    }
}
