/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.Parameterizing;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.PathUtil;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Ted Wilmes (http://twilmes.org)
 */
public final class PrunePathStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final PrunePathStrategy INSTANCE = new PrunePathStrategy();
    private static final Set<Class<? extends OptimizationStrategy>> PRIORS = new HashSet<>();

    static {
        PRIORS.add(PathProcessorStrategy.class);
        PRIORS.add(MatchPredicateStrategy.class);
    }

    private PrunePathStrategy() {
    }

    public static PrunePathStrategy instance() {
        return INSTANCE;
    }

    private void setKeepLabels(final Traversal.Admin<?, ?> traversal, final Set<String> keepLabels) {
        final List<Step> steps = traversal.getSteps();
        for(int i = steps.size() - 1; i >= 0; i--) {
            final Step step = steps.get(i);
            if (step instanceof PathProcessor) {
//                System.out.println("Keeping: " + keepLabels);
                if(keepLabels == null) {
                    ((PathProcessor) step).setKeepLabels(null);
                } else {
                    ((PathProcessor) step).setKeepLabels(new HashSet<>(keepLabels));
                }
            }
            Set<String> referencedLabels = PathUtil.getReferencedLabels(step);
            if(step instanceof MatchStep && step.getNextStep() instanceof EmptyStep) {
                ((PathProcessor) step).setKeepLabels(referencedLabels);
            } else if (step instanceof MatchStep) {
                keepLabels.addAll(referencedLabels);
            }

            if(step instanceof TraversalParent) {
                TraversalParent traversalParent = ((TraversalParent) step);
                final List<Traversal.Admin<?, ?>> subTraversals = new ArrayList<>();
                subTraversals.addAll(traversalParent.getLocalChildren());
                subTraversals.addAll(traversalParent.getGlobalChildren());
                HashSet<String> keepers = new HashSet<>();
                for(final Traversal.Admin<?, ?> subTraversal : subTraversals) {
                    if(keepLabels != null) {
                        keepers.addAll(keepLabels);
                        keepers.addAll(referencedLabels);
                        if(step instanceof MatchStep) {
                            System.out.println(step);
                            keepers.addAll(((MatchStep)step).getMatchStartLabels());
                            keepers.addAll(((MatchStep)step).getMatchEndLabels());
                        }
                        setKeepLabels(subTraversal, keepers);
                    } else {
                        setKeepLabels(subTraversal, null);
                    }
                }
            }
            if(keepLabels != null) {
                keepLabels.addAll(referencedLabels);
            }
        }
    }

//    @Override
    public void apply3(final Traversal.Admin<?, ?> traversal) {
        boolean hasPathStep = false;
        final List<PathStep> pathSteps = TraversalHelper.getStepsOfAssignableClassRecursively(PathStep.class, traversal);
        final List<SubgraphStep> subgraphSteps = TraversalHelper.getStepsOfAssignableClassRecursively(SubgraphStep.class, traversal);
        if(!pathSteps.isEmpty() || !subgraphSteps.isEmpty()) {
            hasPathStep = true;
        }

        // This is a bit weird at this point because the strategy application already happens in a recursive fashion
        // but we shortcut that for this strategy because we want to be able to collect all of the referenced labels
        // at all levels of the traversal and then set the keep labels accordingly for each PathProcessor
        if(!(traversal.getParent() instanceof EmptyStep)) {
            return;
        }

        Set<String> keepLabels = null;
        if(!hasPathStep) {
            keepLabels = new HashSet<>();
        }

        setKeepLabels(traversal, keepLabels);
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        TraversalParent parent = traversal.getParent();

        Set<String> foundLabels = new HashSet<>();
        Set<String> keepLabels = new HashSet<>();

        // If this traversal has a parent, it will need to inherit its
        // parent's keep labels.
        if(!parent.equals(EmptyStep.instance())) {
            // start with parents keep labels
            if(parent instanceof PathProcessor) {
                PathProcessor parentPathProcess = (PathProcessor) parent;
                if(parentPathProcess.getKeepLabels() != null) keepLabels.addAll(parentPathProcess.getKeepLabels());
            }
        }

        // check if the traversal contains any path or subgraph steps and
        boolean hasPathStep = false;
        final List<PathStep> pathSteps = TraversalHelper.getStepsOfAssignableClassRecursively(PathStep.class, traversal);
        final List<SubgraphStep> subgraphSteps = TraversalHelper.getStepsOfAssignableClassRecursively(SubgraphStep.class, traversal);
        if(!pathSteps.isEmpty() || !subgraphSteps.isEmpty()) {
            hasPathStep = true;
        }

        final List<Step> steps = traversal.getSteps();
        for(int i = steps.size() - 1; i >= 0; i--) {
            Step currentStep = steps.get(i);
            if(!hasPathStep) {
                // maintain our list of labels to keep, repeatedly adding labels that were found during
                // the last iteration
                keepLabels.addAll(foundLabels);

                // check child traversals
                if(currentStep instanceof TraversalParent) {
                    TraversalParent traversalParent = (TraversalParent) currentStep;
                    List<Traversal.Admin<Object, Object>> localChildren = traversalParent.getLocalChildren();
                    List<Traversal.Admin<Object, Object>> globalChildren = traversalParent.getGlobalChildren();

                }

                if (currentStep instanceof Parameterizing) {
                    Parameters parameters = ((Parameterizing) currentStep).getParameters();
                    for (Traversal.Admin trav : parameters.getTraversals()) {
                        for (Object ss : trav.getSteps()) {
                            if (ss instanceof Scoping) {
                                Set<String> labels = ((Scoping) ss).getScopeKeys();
                                for (String label : labels) {
                                    if (foundLabels.contains(label)) {
                                        keepLabels.add(label);
                                    } else {
                                        // it'll get added later, b/c we can drop after this step
                                        foundLabels.add(label);
                                    }
                                }
                            }
                        }
                    }
                }

                if (currentStep instanceof Scoping) {
                    Set<String> labels = new HashSet<>(((Scoping) currentStep).getScopeKeys());
                    if (currentStep instanceof MatchStep) {
                        // if this is the last step, keep everything, else just add founds
                        if (currentStep.getNextStep() instanceof EmptyStep) {
                            labels.addAll(((MatchStep) currentStep).getMatchEndLabels());
                            labels.addAll(((MatchStep) currentStep).getMatchStartLabels());
                        }
                    }
                    for (final String label : labels) {
                        if (foundLabels.contains(label)) {
                            keepLabels.add(label);
                        } else {
                            foundLabels.add(label);
                        }
                    }
                }
                if (currentStep instanceof PathProcessor) {
                    if (i != traversal.getSteps().size()) {
                        // add in all match labels
                        ((PathProcessor) currentStep).setKeepLabels(new HashSet<>(foundLabels));
                    }
                    ((PathProcessor) currentStep).setKeepLabels(new HashSet<>(keepLabels));
                }
            } else {
                if (currentStep instanceof PathProcessor) {
                    // set keep labels to null so that no labels are dropped
                    ((PathProcessor) currentStep).setKeepLabels(null);
                }
            }
        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return PRIORS;
    }
}
