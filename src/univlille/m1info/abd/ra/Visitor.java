package univlille.m1info.abd.ra;

public class Visitor implements RAQueryVisitor{

	private int depth = 0;
	
	private void displayInDepth(int treeDepth) {
		for(int i = 0; i < treeDepth; i++)
			System.out.print("\t");
	}

	@Override
	public void visit(SelectionQuery q) {
		System.out.println("Selection");
		depth++;
		displayInDepth(depth);
		q.getSubQuery().accept(this);
	}

	@Override
	public void visit(ProjectionQuery q) {
		System.out.println("Projection");
		q.getSubQuery().accept(this);
	}

	@Override
	public void visit(JoinQuery q) {
		System.out.println("Join");
		depth++;
		displayInDepth(depth);
		q.getLeftSubQuery().accept(this);
		depth = 1;
		displayInDepth(depth);
		q.getRightSubQuery().accept(this);
	}

	@Override
	public void visit(RenameQuery q) {
		System.out.println("Rename");
		depth++;
		displayInDepth(depth);
		q.getSubQuery().accept(this);
	}

	@Override
	public void visit(RelationNameQuery q) {
		System.out.println("Relation");
	}
}
