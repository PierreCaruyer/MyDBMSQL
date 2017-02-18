package univlille.m1info.abd.ra;

public class DisplayVisitor implements RAQueryVisitor{

	@Override
	public void visit(SelectionQuery q) {
		q.getSubQuery().accept(this);
		System.out.println(q);
	}

	@Override
	public void visit(ProjectionQuery q) {
		q.getSubQuery().accept(this);
		System.out.println(q);
	} 

	@Override
	public void visit(JoinQuery q) {
		q.getLeftSubQuery().accept(this);
		q.getRightSubQuery().accept(this);
		System.out.println(q);
	}

	@Override
	public void visit(RenameQuery q) {
		q.getSubQuery().accept(this);
		System.out.println(q);
	}

	@Override
	public void visit(RelationNameQuery q) {
		System.out.println(q);
	}
}
