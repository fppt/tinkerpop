package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectCapStrategy implements TraversalStrategy {

    private static final SideEffectCapStrategy INSTANCE = new SideEffectCapStrategy();

    private SideEffectCapStrategy() {
    }

    @Override
    public void apply(final Traversal<?,?> traversal) {
        final Step endStep = TraversalHelper.getEnd(traversal);
        if (endStep instanceof SideEffectCapable && !(endStep instanceof SideEffectCapStep)) {
            traversal.cap();
        }
    }

    @Override
    public int compareTo(final TraversalStrategy traversalStrategy) {
        return traversalStrategy instanceof TraverserSourceStrategy || traversalStrategy instanceof LabeledEndStepStrategy ? -1 : 1;
    }

    public static SideEffectCapStrategy instance() {
        return INSTANCE;
    }

}
