//! Deliberate file identity operations, not content-similarity inference.
use jj_cli::{
    cli_util::CommandHelper,
    command_error::{CommandError, user_error},
    ui::Ui,
};
use jj_lib::{
    backend::{CopyHistory, TreeValue},
    merge::Merge,
    merged_tree_builder::MergedTreeBuilder,
    object_id::ObjectId as _,
    repo::Repo as _,
};
use serde_json::{Value, json};

#[derive(clap::Args, Clone, Debug)]
pub(crate) struct PathArgs {
    /// Existing tracked regular file, relative to the command directory.
    source: String,
    /// New regular-file path; an existing path is never overwritten.
    destination: String,
}

/// Snapshot through the ordinary command/working-copy fence, then change only
/// the requested tree entries in the normal native rewrite/checkout path.
pub(crate) async fn run(
    ui: &mut Ui,
    command: &CommandHelper,
    args: PathArgs,
    moving: bool,
) -> Result<Value, CommandError> {
    let global = command.global_args();
    if global.no_integrate_operation || global.ignore_working_copy || global.at_operation.is_some()
    {
        return Err(user_error(
            "tracked copy/move requires the current native working copy",
        ));
    }
    let mut helper = command.workspace_helper(ui).await?;
    if helper
        .repo()
        .store()
        .backend_impl::<jj_native_backend::NativeBackend>()
        .is_none()
    {
        return Err(user_error(
            "tracked copy/move requires the native Tinytree backend",
        ));
    }
    let source = helper.parse_file_path(&args.source)?;
    let destination = helper.parse_file_path(&args.destination)?;
    if source == destination
        || source.as_internal_file_string().is_empty()
        || destination.as_internal_file_string().is_empty()
        || source.as_internal_file_string().len() > 4096
        || destination.as_internal_file_string().len() > 4096
    {
        return Err(user_error(
            "tracked copy/move requires two distinct bounded file paths",
        ));
    }
    let commit = helper
        .get_wc_commit()
        .await?
        .ok_or_else(|| user_error("working-copy revision missing"))?;
    helper.check_rewritable([commit.id()]).await?;
    let tree = commit.tree();
    if !tree
        .path_value(&destination)
        .await?
        .as_resolved()
        .is_some_and(Option::is_none)
    {
        return Err(user_error(
            "tracked copy/move destination already exists or is conflicted",
        ));
    }
    let source_value = tree.path_value(&source).await?;
    let Some(Some(TreeValue::File {
        id,
        executable,
        copy_id,
    })) = source_value.as_resolved()
    else {
        return Err(user_error(
            "tracked copy/move source must be a resolved regular file",
        ));
    };
    if copy_id.as_bytes().is_empty() {
        return Err(user_error(
            "source has no retained file identity; submit the source before copying or moving it",
        ));
    }
    // A same-draft chain is not a historical source witness. Require the
    // existing identity in a parent; dirty source content may still be copied.
    let parent = commit.parent_tree(helper.repo().as_ref()).await?;
    let parent_value = parent.path_value(&source).await?;
    if !matches!(parent_value.as_resolved(), Some(Some(TreeValue::File { copy_id: parent_copy, .. })) if parent_copy == copy_id)
    {
        return Err(user_error(
            "source identity is not present in the parent revision; submit it before another tracked copy/move",
        ));
    }
    let original = helper.repo().store().backend().read_copy(copy_id).await?;
    if original.current_path != source {
        return Err(user_error(
            "source file identity does not match its current path",
        ));
    }
    let new_copy = helper
        .repo()
        .store()
        .backend()
        .write_copy(&CopyHistory {
            current_path: destination.clone(),
            parents: vec![copy_id.clone()],
            salt: uuid::Uuid::new_v4().as_bytes().to_vec(),
        })
        .await?;
    let mut builder = MergedTreeBuilder::new(tree);
    builder.set_or_remove(
        destination.clone(),
        Merge::normal(TreeValue::File {
            id: id.clone(),
            executable: *executable,
            copy_id: new_copy.clone(),
        }),
    );
    if moving {
        builder.set_or_remove(source.clone(), Merge::absent());
    }
    let updated = builder.write_tree().await?;
    let mut transaction = helper.start_transaction();
    transaction
        .repo_mut()
        .rewrite_commit(&commit)
        .set_tree(updated)
        .write()
        .await?;
    transaction
        .finish(
            ui,
            if moving {
                "record explicit native file move"
            } else {
                "record explicit native file copy"
            },
        )
        .await?;
    Ok(
        json!({"source":source.as_internal_file_string(),"destination":destination.as_internal_file_string(),"copy_id":new_copy.hex(),"source_copy_id":copy_id.hex()}),
    )
}
